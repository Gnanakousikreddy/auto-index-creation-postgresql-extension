#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

/* ===== Constants ===== */

#define AUTO_INDEX_SHMEM_NAME "auto_index_stats"
#define AUTO_INDEX_HASH_NAME "auto_index_hash"
#define AUTO_INDEX_INITIAL_ENTRIES 1000
#define AUTO_INDEX_COST_THRESHOLD_DEFAULT 1
#define AUTO_INDEX_SELECTIVITY_THRESHOLD_DEFAULT 0.2
#define AUTO_INDEX_WORKER_POLL_INTERVAL 5000	/* 5 seconds */
#define AUTO_INDEX_MAX_RETRIES 3
#define AUTO_INDEX_MAX_NAME_LEN 63			/* PostgreSQL identifier limit */

/* ===== Type Definitions ===== */

/*
 * TrackingKey - Hash table key: uniquely identifies a (table, column) pair
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
	uint64      benefit;            /* Estimated cost benefit of indexing */
	uint64		rows_processed;		/* Total rows scanned */
	uint64		rows_matched;		/* Total rows matching equality predicate */
	bool		triggered;			/* Whether index creation was triggered */

	/* Phase 4: Background worker coordination */
	bool		worker_processing;		/* Currently being processed by worker */
	uint32     creation_attempts;		/* Number of creation attempts */
} TrackingEntry;

/* ===== Global Stats ===== */

typedef struct GlobalStats
{
	int			num_entries;		/* Current tracked entries */
	uint64		total_scans;		/* Total sequential scans tracked */
	uint64		indices_triggered;	/* Total index creation requests */
	uint64		indices_created;	/* Total indices successfully created */
	LWLockPadded *lock;				/* Lock protecting this structure */

	/* Phase 4: Background worker coordination */
	bool		worker_available;		/* Worker process is running */
	TimestampTz last_poll_time;		/* Worker's last poll timestamp */
	uint64		worker_polls;		/* Total number of polls since startup */
	uint64		worker_wakes;		/* Total times worker was woken for work */
} GlobalStats;

/* Global variables (defined in auto_index.c) */
extern GlobalStats *auto_index_stats;
extern struct HTAB *auto_index_hash;
extern double auto_index_selectivity_threshold;
extern bool auto_index_enabled;
extern int auto_index_max_workers;
extern char *auto_index_database_name;

/* Background worker entry point */
extern void auto_index_worker_main(Datum arg);

#endif							/* AUTO_INDEX_H */
