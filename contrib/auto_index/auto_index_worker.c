#include "postgres.h"
#include "auto_index.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/skey.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "commands/sequence.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "utils/snapmgr.h"

#include <sys/time.h>

static void auto_index_worker_sighup(SIGNAL_ARGS);
static void auto_index_worker_sigterm(SIGNAL_ARGS);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static void
auto_index_worker_sighup(SIGNAL_ARGS)
{
	int	saved_errno = errno;

	got_sighup = true;
	if (MyLatch)
		SetLatch(MyLatch);

	errno = saved_errno;
}

static void
auto_index_worker_sigterm(SIGNAL_ARGS)
{
	int	saved_errno = errno;

	got_sigterm = true;
	if (MyLatch)
		SetLatch(MyLatch);

	errno = saved_errno;
}

static char *
auto_index_generate_index_name(Oid rel_oid, AttrNumber attr_no)
{
	char	   *index_name;

	index_name = palloc(256);
	snprintf(index_name, 256, "idx_auto_%u_%d", rel_oid, attr_no);

	return index_name;
}


static bool
auto_index_create_index(TrackingEntry *entry)
{
	char	   *relname;
	char	   *index_name;
	char	   *sql;
	char	   *check_sql;
	bool		result = false;
	int			ret;
	bool index_exists;

	if (!entry)
		return false;

	index_name = auto_index_generate_index_name(entry->key.rel_oid, entry->key.attr_no);
	if (!index_name)
	{
		ereport(LOG, (errmsg("auto_index: failed to generate index name for rel %u, attr %d",
							 entry->key.rel_oid, entry->key.attr_no)));
		return false;
	}

	relname = get_rel_name(entry->key.rel_oid);
	if (!relname)
	{
		ereport(LOG, (errmsg("auto_index: failed to get relation name for OID %u",
							 entry->key.rel_oid)));
		pfree(index_name);
		return false;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(LOG, (errmsg("auto_index: failed to connect to SPI for index %s", index_name)));
		pfree(relname);
		pfree(index_name);
		return false;
	}

    PushActiveSnapshot(GetTransactionSnapshot());
	check_sql = psprintf("SELECT 1 FROM pg_class WHERE relname = '%s'", index_name);
    ret = SPI_execute(check_sql, true, 0);
	index_exists = (ret == SPI_OK_SELECT && SPI_processed > 0);
    pfree(check_sql);

	if (index_exists)
    {
		ereport(LOG, (errmsg("auto_index: index %s already exists, skipping creation",
							index_name)));
	
        SPI_finish();
        pfree(relname);
        pfree(index_name);
		PopActiveSnapshot();
        return true; 
    }

	ereport(LOG, (errmsg("auto_index: creating index %s on %s(%s)",
							index_name, relname,
							get_attname(entry->key.rel_oid,
									entry->key.attr_no, true))));

	sql = psprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				  index_name, relname,
				  get_attname(entry->key.rel_oid, entry->key.attr_no, true));

	ret = SPI_execute(sql, false, 0);

	if (ret < 0)
	{
		ereport(LOG, (errmsg("auto_index: SPI_execute failed with code %d for index %s",
							 ret, index_name)));
		ereport(LOG, (errmsg("auto_index: SQL was: %s", sql)));
	}
	else if (ret != SPI_OK_UTILITY)
	{
		ereport(LOG, (errmsg("auto_index: index creation %s failed with status code %d",
							 index_name, ret)));
		ereport(LOG, (errmsg("auto_index: SQL was: %s", sql)));
	}
	else
	{
		result = true;
		entry->triggered = false;
		entry->worker_processing = false;
		entry->creation_attempts = 0;

		LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
		auto_index_stats->indices_created++;
		LWLockRelease((LWLock *) auto_index_stats->lock);

		ereport(LOG, (errmsg("auto_index: successfully created index %s on %s(%s)",
							 index_name, relname,
							 get_attname(entry->key.rel_oid,
									   entry->key.attr_no, true))));
	}

    PopActiveSnapshot();

	SPI_finish();

	pfree(sql);
	pfree(relname);
	pfree(index_name);

	return result;
}


static void
auto_index_process_pending_indices(void)
{
	HASH_SEQ_STATUS status;
	TrackingEntry *entry;
	int			processed = 0;
	int			created = 0;
	bool 		rel_exists;

	if (!auto_index_stats || !auto_index_hash)
		return;

	StartTransactionCommand();
	hash_seq_init(&status, auto_index_hash);

	while ((entry = (TrackingEntry *) hash_seq_search(&status)) != NULL)
	{
		
        rel_exists = SearchSysCacheExists1(RELOID, ObjectIdGetDatum(entry->key.rel_oid));
		if (!rel_exists)
		{
			ereport(LOG, (errmsg("auto_index: relation %u has been dropped, removing entry",
								entry->key.rel_oid)));
			
			hash_search(auto_index_hash, &entry->key, HASH_REMOVE, NULL);
			LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
			auto_index_stats->num_entries--;
			LWLockRelease((LWLock *) auto_index_stats->lock);
			continue;
		}

		
		if (entry->triggered && !entry->worker_processing &&
			entry->creation_attempts < AUTO_INDEX_MAX_RETRIES)
		{
			entry->worker_processing = true;
			entry->creation_attempts++;

			ereport(LOG, (errmsg("auto_index: processing index creation for rel %u, attr %d (attempt %d)",
									entry->key.rel_oid, entry->key.attr_no,
									entry->creation_attempts)));
			
			if (auto_index_create_index(entry))
			{
				created++;
				hash_search(auto_index_hash, &entry->key, HASH_REMOVE, NULL);
				LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
				auto_index_stats->num_entries--;
				LWLockRelease((LWLock *) auto_index_stats->lock);
			}
			else
			{
				entry->worker_processing = false;
				ereport(LOG, (errmsg("auto_index: index creation failed, will retry on next poll")));
			}

			processed++;
		}
	}

	CommitTransactionCommand();

	if (processed > 0)
	{
		ereport(LOG, (errmsg("auto_index: worker processed %d entries, created %d indices",
							 processed, created)));
	}
}


PGDLLEXPORT void
auto_index_worker_main(Datum arg)
{
	int	rc;

	pqsignal(SIGHUP, auto_index_worker_sighup);
	pqsignal(SIGTERM, auto_index_worker_sigterm);
	pqsignal(SIGINT, auto_index_worker_sigterm);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(auto_index_database_name, NULL, 0);

	LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
	auto_index_stats->worker_available = true;
	LWLockRelease((LWLock *) auto_index_stats->lock);

	ereport(LOG, (errmsg("auto_index: background worker started")));

	for (;;)
	{
		if (got_sigterm)
		{
			ereport(LOG, (errmsg("auto_index: background worker received SIGTERM, shutting down")));
			break;
		}

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
			ereport(LOG, (errmsg("auto_index: reloaded configuration")));
		}

		if (auto_index_enabled && auto_index_stats && auto_index_hash)
		{
			auto_index_process_pending_indices();
		}

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   AUTO_INDEX_WORKER_POLL_INTERVAL, PG_WAIT_EXTENSION);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}

		if (rc & WL_EXIT_ON_PM_DEATH)
		{
			break;
		}

		CHECK_FOR_INTERRUPTS();
	}

	LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
	auto_index_stats->worker_available = false;
	LWLockRelease((LWLock *) auto_index_stats->lock);

	ereport(LOG, (errmsg("auto_index: background worker stopped")));

	proc_exit(0);
}