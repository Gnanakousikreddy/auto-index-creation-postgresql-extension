#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "access/attnum.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"
#define AUTO_INDEX_SHMEM_NAME "auto_index_stats"
#define AUTO_INDEX_HASH_NAME "auto_index_hash"
#define AUTO_INDEX_INITIAL_ENTRIES 1000
#define AUTO_INDEX_WORKER_POLL_INTERVAL 5000	
#define AUTO_INDEX_MAX_RETRIES 3
#define AUTO_INDEX_MAX_NAME_LEN 63			

typedef struct TrackingKey
{
	Oid			rel_oid;			
	AttrNumber	attr_no;			
} TrackingKey;


typedef struct TrackingEntry
{
	TrackingKey key;				
	uint64		scan_count;		
	uint64      benefit;            
	bool		triggered;			

	bool		worker_processing;		
	uint32     creation_attempts;		
} TrackingEntry;



typedef struct GlobalStats
{
	int			num_entries;		
	uint64		total_scans;		
	uint64		indices_triggered;	
	uint64		indices_created;	
	LWLockPadded *lock;			
	bool		worker_available;		
} GlobalStats;

extern GlobalStats *auto_index_stats;
extern struct HTAB *auto_index_hash;
extern bool auto_index_enabled;
extern char *auto_index_database_name;
extern void auto_index_worker_main(Datum arg);

#endif						
