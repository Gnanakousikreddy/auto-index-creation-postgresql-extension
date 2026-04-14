/*-------------------------------------------------------------------------
 *
 * auto_index.c
 *    Autonomous Index Creation Infrastructure - Phase 2
 *
 * Tracks sequential scans with equality predicates and maintains statistics
 * for automatic index creation trigger decisions.
 *
 * Architecture:
 * - Shared memory: hash table indexed by (rel_oid, attr_no) pairs
 * - Executor hook: captures sequential scans with cost information
 * - Predicate analysis: extracts indexed columns from scan predicates
 * - Threshold logic: triggers index creation when cost threshold exceeded
 *
 * Portions Copyright (c) 2026, CS349 Project Team
 * Based on PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    contrib/auto_index/auto_index.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* Must be first for SQL declarations */
PG_MODULE_MAGIC;

/* ===== Configuration Constants ===== */

#define AUTO_INDEX_MAX_ENTRIES 1000
#define AUTO_INDEX_COST_THRESHOLD 1000
#define AUTO_INDEX_SELECTIVITY_THRESHOLD 0.2

/* ===== Shared Memory Structure Definitions ===== */

/*
 * TrackingKey - Hash table key: (relation OID, attribute number)
 * Uniquely identifies a (table, column) pair being tracked
 */
typedef struct TrackingKey
{
	Oid			rel_oid;			/* Table OID */
	AttrNumber	attr_no;			/* Column attribute number */
} TrackingKey;

/*
 * TrackingEntry - Hash table value: statistics for one (table, column) pair
 * Maintains accumulated cost and selectivity information
 */
typedef struct TrackingEntry
{
	TrackingKey key;				/* Embedded key */
	uint64		scan_count;			/* Number of sequential scans */
	uint64		accumulated_cost;	/* Total estimated cost from planner */
	uint64		rows_processed;		/* Total rows scanned */
	uint64		rows_matched;		/* Total rows matching equality predicate */
	bool		triggered;			/* Whether index creation was triggered */
} TrackingEntry;

/*
 * GlobalStats - Shared memory header with global statistics
 */
typedef struct GlobalStats
{
	int			num_entries;		/* Current tracked entries */
	uint64		total_scans;		/* Total sequential scans tracked */
	uint64		indices_triggered;	/* Total index creation requests */
	uint64		indices_created;	/* Total indices successfully created */
	LWLockPadded *lock;				/* Lock protecting this structure */
} GlobalStats;

/* ===== Global State (Process-local) ===== */

static GlobalStats *auto_index_stats = NULL;
static HTAB *auto_index_hash = NULL;

/* GUC Parameters */
static int auto_index_cost_threshold = AUTO_INDEX_COST_THRESHOLD;
static double auto_index_selectivity_threshold = AUTO_INDEX_SELECTIVITY_THRESHOLD;
static bool auto_index_enabled = true;
static int auto_index_max_workers = 4;
static bool auto_index_debug = false;

/* Hook variables */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_executor_start_hook = NULL;

/* ===== Function Declarations ===== */

void _PG_init(void);
void _PG_fini(void);

static void auto_index_shmem_request(void);
static void auto_index_shmem_startup(void);
static void auto_index_executor_start(QueryDesc *queryDesc, int eflags);

static void AutoIndexTrackSeqscan(Oid rel_oid, const char *rel_name,
								  Cost cost, uint64 rows_processed, uint64 rows_matched,
								  Bitmapset *indexed_attrs);
static void AutoIndexUpdateEntry(TrackingEntry *entry, Cost cost,
								  uint64 rows_processed, uint64 rows_matched);
static bool AutoIndexCheckThreshold(TrackingEntry *entry);
static double AutoIndexCalculateSelectivity(const TrackingEntry *entry);
static void AutoIndexLogStats(void);

/* ===== Module Initialization ===== */

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Setup shared memory hooks */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = auto_index_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = auto_index_shmem_startup;

	/* Setup executor hook */
	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = auto_index_executor_start;

	/* Register GUC parameters */
	DefineCustomIntVariable(
		"auto_index.cost_threshold",
		"Cost threshold for triggering autonomous indexing",
		"When accumulated sequential scan cost exceeds this value, "
		"an index creation is triggered (in planner cost units).",
		&auto_index_cost_threshold,
		AUTO_INDEX_COST_THRESHOLD,
		100,
		INT_MAX,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomRealVariable(
		"auto_index.selectivity_threshold",
		"Selectivity threshold for index creation",
		"Predicates with selectivity above this threshold are not indexed.",
		&auto_index_selectivity_threshold,
		AUTO_INDEX_SELECTIVITY_THRESHOLD,
		0.0,
		1.0,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"auto_index.enabled",
		"Enable autonomous index creation",
		"When enabled, the system automatically tracks sequential scans "
		"and creates indices for frequently scanned columns.",
		&auto_index_enabled,
		true,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"auto_index.max_workers",
		"Maximum concurrent autonomous index creation workers",
		"Limits the number of background workers that can "
		"concurrently create indices.",
		&auto_index_max_workers,
		4,
		1,
		32,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"auto_index.debug",
		"Enable debug logging for sequential scans",
		"When enabled, logs details about tracked sequential scans.",
		&auto_index_debug,
		false,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	ereport(LOG, (errmsg("auto_index extension loaded")));
}

void
_PG_fini(void)
{
	/* Restore previous hooks */
	shmem_request_hook = prev_shmem_request_hook;
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_executor_start_hook;
}

/* ===== Shared Memory Initialization ===== */

static void
auto_index_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	if (!auto_index_enabled)
		return;

	/* Request space for global stats and hash table */
	RequestAddinShmemSpace(add_size(sizeof(GlobalStats),
								hash_estimate_size(AUTO_INDEX_MAX_ENTRIES,
													 sizeof(TrackingEntry))));

	/* Request LWLock tranche for synchronization */
	RequestNamedLWLockTranche("AutoIndex", 1);
}

static void
auto_index_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	int			lock_tranche_id;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	if (!auto_index_enabled)
		return;

	/* Initialize global stats structure */
	auto_index_stats = (GlobalStats *)
		ShmemInitStruct("AutoIndexStats",
					   sizeof(GlobalStats),
					   &found);

	if (!found)
	{
		/* First process to initialize */
		auto_index_stats->num_entries = 0;
		auto_index_stats->total_scans = 0;
		auto_index_stats->indices_triggered = 0;
		auto_index_stats->indices_created = 0;

		/* Get and assign LWLock */
		lock_tranche_id = LWLockNewTrancheId();
		auto_index_stats->lock = &(GetNamedLWLockTranche("AutoIndex"))[0];
		LWLockInitialize(auto_index_stats->lock, lock_tranche_id);
	}

	/* Initialize hash table for tracking (rel_oid, attr_no) pairs */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TrackingKey);
	info.entrysize = sizeof(TrackingEntry);
	info.hash = tag_hash;
	info.num_partitions = 16;

	auto_index_hash = ShmemInitHash(
		"AutoIndexTable",
		AUTO_INDEX_MAX_ENTRIES,
		AUTO_INDEX_MAX_ENTRIES,
		&info,
		HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);

	ereport(LOG, (errmsg("auto_index: shared memory initialized")));
}

/* ===== Executor Hook Implementation ===== */

static void
auto_index_executor_start(QueryDesc *queryDesc, int eflags)
{
	/* Call previous hook first */
	if (prev_executor_start_hook)
		prev_executor_start_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/* Note: Actual sequential scan tracking happens during execution,
	   not here. This hook is a placeholder for potential future enhancements
	   like query plan analysis. */
}

/* ===== Tracking Functions ===== */

/*
 * AutoIndexTrackSeqscan
 *
 * Called to record a sequential scan for a relation. This function:
 * 1. Identifies indexed columns from predicates (attr_no)
 * 2. Updates or creates tracking entries for each (rel_oid, attr_no) pair
 * 3. Accumulates cost information
 * 4. Checks if index creation threshold is exceeded
 *
 * Parameters:
 *   rel_oid: OID of the relation being scanned
 *   rel_name: Name of relation (for logging)
 *   cost: Estimated cost of this sequential scan
 *   rows_processed: Number of rows scanned
 *   rows_matched: Number of rows matching the predicate
 *   indexed_attrs: Bitmap of attribute numbers to track
 */
static void
AutoIndexTrackSeqscan(Oid rel_oid, const char *rel_name,
					  Cost cost, uint64 rows_processed, uint64 rows_matched,
					  Bitmapset *indexed_attrs)
{
	TrackingKey key;
	TrackingEntry *entry;
	bool		found;
	int			attr_no = -1;

	if (!auto_index_enabled || auto_index_hash == NULL || !indexed_attrs)
		return;

	/* Iterate over all indexed attributes in the bitmap */
	while ((attr_no = bms_next_member(indexed_attrs, attr_no)) >= 0)
	{
		key.rel_oid = rel_oid;
		key.attr_no = attr_no;

		/* Find or create tracking entry */
		LWLockAcquire(auto_index_stats->lock, LW_EXCLUSIVE);
		{
			entry = (TrackingEntry *) hash_search(auto_index_hash, &key,
												   HASH_ENTER, &found);

			if (!found)
			{
				/* Initialize new entry */
				entry->key = key;
				entry->scan_count = 0;
				entry->accumulated_cost = 0;
				entry->rows_processed = 0;
				entry->rows_matched = 0;
				entry->triggered = false;
				auto_index_stats->num_entries++;
			}

			/* Update tracking statistics */
			AutoIndexUpdateEntry(entry, cost, rows_processed, rows_matched);
			auto_index_stats->total_scans++;

			/* Check if threshold exceeded */
			if (!entry->triggered && AutoIndexCheckThreshold(entry))
			{
				entry->triggered = true;
				auto_index_stats->indices_triggered++;

				if (auto_index_debug)
					ereport(LOG,
							(errmsg("auto_index: threshold exceeded for (%u, %d) on table %s",
									rel_oid, attr_no, rel_name)));
			}
		}
		LWLockRelease(auto_index_stats->lock);
	}
}

/*
 * AutoIndexUpdateEntry
 *
 * Updates tracking entry with new scan statistics
 */
static void
AutoIndexUpdateEntry(TrackingEntry *entry, Cost cost,
					  uint64 rows_processed, uint64 rows_matched)
{
	entry->scan_count++;
	entry->accumulated_cost += (uint64) cost;
	entry->rows_processed += rows_processed;
	entry->rows_matched += rows_matched;
}

/*
 * AutoIndexCheckThreshold
 *
 * Determines if index creation should be triggered based on:
 * - Accumulated cost exceeding threshold
 * - Selectivity above minimum threshold
 */
static bool
AutoIndexCheckThreshold(TrackingEntry *entry)
{
	double		selectivity;

	/* Check cost threshold */
	if (entry->accumulated_cost <= (uint64) auto_index_cost_threshold)
		return false;

	/* Check selectivity threshold */
	selectivity = AutoIndexCalculateSelectivity(entry);
	if (selectivity > auto_index_selectivity_threshold)
		return false;				/* Too selective, don't index */

	return true;
}

/*
 * AutoIndexCalculateSelectivity
 *
 * Calculates the selectivity of predicates on this column
 * Returns ratio of matched rows to processed rows
 */
static double
AutoIndexCalculateSelectivity(const TrackingEntry *entry)
{
	if (entry->rows_processed == 0)
		return 1.0;

	return (double) entry->rows_matched / (double) entry->rows_processed;
}

/*
 * AutoIndexLogStats
 *
 * Logs current tracking statistics (for debugging/monitoring)
 */
static void
AutoIndexLogStats(void)
{
	if (!auto_index_enabled || auto_index_stats == NULL)
		return;

	ereport(LOG,
			(errmsg("auto_index stats: entries=%d, scans=%lu, triggered=%lu, created=%lu",
					auto_index_stats->num_entries,
					auto_index_stats->total_scans,
					auto_index_stats->indices_triggered,
					auto_index_stats->indices_created)));
}

/* ===== SQL-Callable Functions ===== */

/*
 * get_auto_index_stats() returns TABLE (...)
 * Returns current tracking statistics
 */
PG_FUNCTION_INFO_V1(get_auto_index_stats);

Datum
get_auto_index_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	HeapTuple	tuple;
	Datum		result;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	BlessTupleDesc(tupdesc);

	if (auto_index_stats == NULL)
	{
		/* Extension not enabled or not initialized */
		values[0] = Int32GetDatum(0);
		values[1] = Int64GetDatum(0);
		values[2] = Int64GetDatum(0);
		values[3] = Int64GetDatum(0);
	}
	else
	{
		LWLockAcquire(auto_index_stats->lock, LW_SHARED);
		{
			values[0] = Int32GetDatum(auto_index_stats->num_entries);
			values[1] = Int64GetDatum(auto_index_stats->total_scans);
			values[2] = Int64GetDatum(auto_index_stats->indices_triggered);
			values[3] = Int64GetDatum(auto_index_stats->indices_created);
		}
		LWLockRelease(auto_index_stats->lock);
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}
