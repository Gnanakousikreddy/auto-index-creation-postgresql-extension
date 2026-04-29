/*-------------------------------------------------------------------------
 *
 * auto_index_worker.c
 *    Background Worker for Autonomous Index Creation - Phase 4
 *
 * This worker polls the shared memory hash table for entries that have
 * been triggered for index creation, and executes CREATE INDEX CONCURRENTLY
 * to create the index without blocking user queries.
 *
 * Architecture:
 * - Single persistent worker registered in _PG_init()
 * - Polls every 5 seconds for pending index creation tasks
 * - Uses SPI to execute CREATE INDEX CONCURRENTLY
 * - Clears triggered flag on success or error
 *
 * Portions Copyright (c) 2026, CS349 Project Team
 *
 * IDENTIFICATION
 *    contrib/auto_index/auto_index_worker.c
 *
 *-------------------------------------------------------------------------
 */
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

/* Forward declarations */
static void auto_index_worker_sighup(SIGNAL_ARGS);
static void auto_index_worker_sigterm(SIGNAL_ARGS);

/* ===== Shared Memory Access ===== */
/* These are defined in auto_index.c and declared in auto_index.h */

/* ===== Worker State ===== */

// static Latch worker_latch;
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* ===== Signal Handlers ===== */

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

	/* Build index name: idx_auto_<relid>_<attrno> */
	index_name = palloc(256);
	snprintf(index_name, 256, "idx_auto_%u_%d", rel_oid, attr_no);

	return index_name;
}

/* ===== Index Creation ===== */

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

	/* Generate index name */
	index_name = auto_index_generate_index_name(entry->key.rel_oid, entry->key.attr_no);
	if (!index_name)
	{
		ereport(LOG, (errmsg("auto_index: failed to generate index name for rel %u, attr %d",
							 entry->key.rel_oid, entry->key.attr_no)));
		return false;
	}

	/* Get relation name */
	relname = get_rel_name(entry->key.rel_oid);
	if (!relname)
	{
		ereport(LOG, (errmsg("auto_index: failed to get relation name for OID %u",
							 entry->key.rel_oid)));
		pfree(index_name);
		return false;
	}

	/* Connect to SPI */
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(LOG, (errmsg("auto_index: failed to connect to SPI for index %s", index_name)));
		pfree(relname);
		pfree(index_name);
		return false;
	}

	// Activate a snapshot so SPI can read tables
    PushActiveSnapshot(GetTransactionSnapshot());
	/* Check if index already exists in pg_class */
	check_sql = psprintf("SELECT 1 FROM pg_class WHERE relname = '%s'", index_name);
    ret = SPI_execute(check_sql, true, 0);
	index_exists = (ret == SPI_OK_SELECT && SPI_processed > 0);
    pfree(check_sql);
	// Deactivate the snapshot when done reading

	if (index_exists)
    {
		ereport(LOG, (errmsg("auto_index: index %s already exists, skipping creation",
							index_name)));
	
        /* Clean up and return success */
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

	/* Build CREATE INDEX CONCURRENTLY statement */
	sql = psprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				  index_name, relname,
				  get_attname(entry->key.rel_oid, entry->key.attr_no, true));

	/* Execute via SPI */
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
		/* Success */
		result = true;
		entry->triggered = false;
		entry->worker_processing = false;
		entry->creation_attempts = 0;

		/* Update global stats */
		LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
		auto_index_stats->indices_created++;
		auto_index_stats->worker_wakes++;
		LWLockRelease((LWLock *) auto_index_stats->lock);

		ereport(LOG, (errmsg("auto_index: successfully created index %s on %s(%s)",
							 index_name, relname,
							 get_attname(entry->key.rel_oid,
									   entry->key.attr_no, true))));
	}

    PopActiveSnapshot();

	/* Disconnect SPI */
	SPI_finish();

	/* Cleanup */
	pfree(sql);
	pfree(relname);
	pfree(index_name);

	return result;
}

/* ===== Task Processing ===== */

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

	/* Update poll timestamp */
	LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
	if (auto_index_stats->worker_available)
	{
		auto_index_stats->last_poll_time = GetCurrentTimestamp();
		auto_index_stats->worker_polls++;
	}
	LWLockRelease((LWLock *) auto_index_stats->lock);

	StartTransactionCommand();
	/* Iterate through hash table looking for triggered entries */
	hash_seq_init(&status, auto_index_hash);

	while ((entry = (TrackingEntry *) hash_seq_search(&status)) != NULL)
	{
		/* Check if the relation has been dropped */
		
        rel_exists = SearchSysCacheExists1(RELOID, ObjectIdGetDatum(entry->key.rel_oid));
		if (!rel_exists)
		{
			ereport(LOG, (errmsg("auto_index: relation %u has been dropped, removing entry",
								entry->key.rel_oid)));
			
			/* Remove this entry from the hash table */
			hash_search(auto_index_hash, &entry->key, HASH_REMOVE, NULL);
			LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
			auto_index_stats->num_entries--;
			LWLockRelease((LWLock *) auto_index_stats->lock);
			continue;
		}

		/*
		 * Check if this entry needs index creation:
		 * - triggered = true (threshold exceeded)
		 * - worker_processing = false (not already being processed)
		 * - creation_attempts < MAX_RETRIES (haven't given up)
		 */
		if (entry->triggered && !entry->worker_processing &&
			entry->creation_attempts < AUTO_INDEX_MAX_RETRIES)
		{
			/* Mark as being processed to prevent race conditions */
			entry->worker_processing = true;
			entry->creation_attempts++;

			ereport(LOG, (errmsg("auto_index: processing index creation for rel %u, attr %d (attempt %d)",
									entry->key.rel_oid, entry->key.attr_no,
									entry->creation_attempts)));
			
			/* Create the index */
			if (auto_index_create_index(entry))
			{
				created++;
				/* Remove entry from hash table after successful creation */
				hash_search(auto_index_hash, &entry->key, HASH_REMOVE, NULL);
				LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
				auto_index_stats->num_entries--;
				LWLockRelease((LWLock *) auto_index_stats->lock);
			}
			else
			{
				/* Clear the flags to allow retry on next poll */
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

/* ===== Main Worker Entry Point ===== */

PGDLLEXPORT void
auto_index_worker_main(Datum arg)
{
	int	rc;

	/* Register signal handlers before doing anything else */
	pqsignal(SIGHUP, auto_index_worker_sighup);
	pqsignal(SIGTERM, auto_index_worker_sigterm);
	pqsignal(SIGINT, auto_index_worker_sigterm);

	/* Initialize current process as background worker */
	BackgroundWorkerUnblockSignals();

	/* Connect to the database - use postgres as default database */
	BackgroundWorkerInitializeConnection(auto_index_database_name, NULL, 0);

	/* Mark worker as available */
	LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
	auto_index_stats->worker_available = true;
	auto_index_stats->worker_polls = 0;
	auto_index_stats->worker_wakes = 0;
	LWLockRelease((LWLock *) auto_index_stats->lock);

	ereport(LOG, (errmsg("auto_index: background worker started")));

	/* Main loop */
	for (;;)
	{
		/* Check for shutdown signal */
		if (got_sigterm)
		{
			ereport(LOG, (errmsg("auto_index: background worker received SIGTERM, shutting down")));
			break;
		}

		/* Reload configuration on SIGHUP */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
			ereport(LOG, (errmsg("auto_index: reloaded configuration")));
		}

		/* Process pending index creation tasks */
		if (auto_index_enabled && auto_index_stats && auto_index_hash)
		{
			auto_index_process_pending_indices();
		}

		/* Wait for next poll interval or signal */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   AUTO_INDEX_WORKER_POLL_INTERVAL, PG_WAIT_EXTENSION);

		/* Handle latch being set */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}

		/* Check for exit on postmaster death */
		if (rc & WL_EXIT_ON_PM_DEATH)
		{
			break;
		}

		/* Check for interrupts */
		CHECK_FOR_INTERRUPTS();
	}

	/* Clean shutdown */
	LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
	auto_index_stats->worker_available = false;
	LWLockRelease((LWLock *) auto_index_stats->lock);

	ereport(LOG, (errmsg("auto_index: background worker stopped")));

	proc_exit(0);
}