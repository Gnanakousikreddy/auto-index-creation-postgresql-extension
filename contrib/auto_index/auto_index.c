#include "postgres.h"
#include "auto_index.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/table.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "optimizer/optimizer.h"
#include "optimizer/cost.h"
#include "parser/parser.h"
#include "tcop/tcopprot.h"
#include <math.h>

#include <dlfcn.h>

PG_MODULE_MAGIC;

#define AUTO_INDEX_MAX_ENTRIES 1000

GlobalStats *auto_index_stats = NULL;
HTAB *auto_index_hash = NULL;
static bool in_hook = false;


bool auto_index_enabled = true;
char *auto_index_database_name = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;


void _PG_init(void);
void _PG_fini(void);

PGDLLEXPORT Datum get_auto_index_stats(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum get_auto_index_entries(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum reset_auto_index(PG_FUNCTION_ARGS);

static void auto_index_shmem_request(void);
static void auto_index_shmem_startup(void);
static void auto_index_executor_start(QueryDesc *queryDesc, int eflags);
static void auto_index_executor_end(QueryDesc *queryDesc);
static void auto_index_walk_planstate_tree(PlanState *planstate, QueryDesc *queryDesc);

static Bitmapset *auto_index_extract_equality_cols(Node *qual);
static AttrNumber auto_index_extract_var_attno(const Var *var);
static bool auto_index_is_equality_predicate(const OpExpr *expr);

static void AutoIndexTrackSeqscan(Oid rel_oid, const char *rel_name, uint64 benefit, Bitmapset *indexed_attrs);
static void AutoIndexUpdateEntry(TrackingEntry *entry, uint64 benefit);
static bool AutoIndexCheckThreshold(TrackingEntry *entry);


static double estimate_index_creation_cost(Oid relid);
static double get_hypopg_benefit(QueryDesc *queryDesc, Oid relid, AttrNumber attno);
static void auto_index_analyze_seqscan_state(SeqScanState *seqscan_state, Relation rel, QueryDesc *queryDesc);


void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = auto_index_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = auto_index_shmem_startup;

	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = auto_index_executor_start;

	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = auto_index_executor_end;

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

	DefineCustomStringVariable(
		"auto_index.database_name",
		"Database name for autonomous index creation worker",
		"Specifies the database that the background worker should connect to for index creation. "
		"If not set, defaults to the current database.",
		&auto_index_database_name,
		"test_db",
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	{
		BackgroundWorker worker;
		memset(&worker, 0, sizeof(BackgroundWorker));
		snprintf(worker.bgw_name, BGW_MAXLEN, "auto_index worker");
		snprintf(worker.bgw_type, BGW_MAXLEN, "auto_index");
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(worker.bgw_library_name, MAXPGPATH, "auto_index");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_worker_main");
		worker.bgw_main_arg = 0;
		RegisterBackgroundWorker(&worker);
	}

	ereport(LOG, (errmsg("auto_index extension loaded")));
}

void
_PG_fini(void)
{
	shmem_request_hook = prev_shmem_request_hook;
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_executor_start_hook;
	ExecutorEnd_hook = prev_executor_end_hook;
}


static void
auto_index_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	if (!auto_index_enabled)
		return;

	RequestAddinShmemSpace(add_size(sizeof(GlobalStats),
								hash_estimate_size(AUTO_INDEX_MAX_ENTRIES,
													 sizeof(TrackingEntry))));

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

	auto_index_stats = (GlobalStats *)
		ShmemInitStruct("AutoIndexStats",
					   sizeof(GlobalStats),
					   &found);

	if (!found)
	{
		auto_index_stats->num_entries = 0;
		auto_index_stats->total_scans = 0;
		auto_index_stats->indices_triggered = 0;
		auto_index_stats->indices_created = 0;

		auto_index_stats->worker_available = false;

		lock_tranche_id = LWLockNewTrancheId();
		auto_index_stats->lock = &(GetNamedLWLockTranche("AutoIndex"))[0];
		LWLockInitialize((LWLock *) auto_index_stats->lock, lock_tranche_id);
	}

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

static void
auto_index_executor_start(QueryDesc *queryDesc, int eflags)
{
    if (in_hook)
    {
        if (prev_executor_start_hook)
            prev_executor_start_hook(queryDesc, eflags);
        else
            standard_ExecutorStart(queryDesc, eflags);
        return;
    }

    in_hook = true;
    PG_TRY();
    {
       
        if (auto_index_enabled && queryDesc != NULL)
        {
            queryDesc->instrument_options |= INSTRUMENT_ROWS;
        }

        if (prev_executor_start_hook)
            prev_executor_start_hook(queryDesc, eflags);
        else
            standard_ExecutorStart(queryDesc, eflags);
    }
    PG_FINALLY();
    {
        in_hook = false;
    }
    PG_END_TRY();
}

static void
auto_index_executor_end(QueryDesc *queryDesc)
{
    if (in_hook)
    {
        if (prev_executor_end_hook)
            prev_executor_end_hook(queryDesc);
        else
            standard_ExecutorEnd(queryDesc);
        return;
    }

    in_hook = true;
    PG_TRY();
    {
        if (auto_index_enabled && queryDesc != NULL && queryDesc->planstate != NULL &&
            queryDesc->plannedstmt != NULL &&
            (queryDesc->plannedstmt->commandType == CMD_SELECT ||
             queryDesc->plannedstmt->commandType == CMD_INSERT ||
             queryDesc->plannedstmt->commandType == CMD_UPDATE ||
             queryDesc->plannedstmt->commandType == CMD_DELETE))
        {
            auto_index_walk_planstate_tree(queryDesc->planstate, queryDesc);
        }

        if (prev_executor_end_hook)
            prev_executor_end_hook(queryDesc);
        else
            standard_ExecutorEnd(queryDesc);
    }
    PG_FINALLY();
    {
        in_hook = false;
    }
    PG_END_TRY();
}


static void
auto_index_analyze_seqscan_state(SeqScanState *seqscan_state, Relation rel, QueryDesc *queryDesc) {
    
	SeqScan         *seqscan;
    Bitmapset       *indexed_cols;
    Cost             benefit;

    if (seqscan_state == NULL || rel == NULL)
        return;

    seqscan = (SeqScan *) seqscan_state->ss.ps.plan;
    indexed_cols = auto_index_extract_equality_cols((Node *) seqscan->scan.plan.qual);
    
    if (indexed_cols == NULL)
        return;


    benefit = (Cost) get_hypopg_benefit(queryDesc, rel->rd_id, bms_next_member(indexed_cols, -1));

    if (benefit <= 0) {
        bms_free(indexed_cols);
        return;
    }

	ereport(LOG,
			(errmsg("auto_index: HypoPG Benefit for %s: %.2f", 
                    RelationGetRelationName(rel), (double)benefit)));
	
    AutoIndexTrackSeqscan(rel->rd_id, RelationGetRelationName(rel), (uint64)benefit, indexed_cols);

    bms_free(indexed_cols);
}


static double
estimate_index_creation_cost(Oid relid)
{
    Relation rel;
    double nblocks;
    double tuples;
    double build_cost;

    rel = table_open(relid, AccessShareLock);
    nblocks = (double) RelationGetNumberOfBlocks(rel);
    tuples = (double) rel->rd_rel->reltuples;
    table_close(rel, AccessShareLock);

    if (tuples <= 0) tuples = 1000;

    build_cost = (nblocks * seq_page_cost) + 
                 (tuples * log2(tuples + 1.0) * cpu_operator_cost);

    return build_cost;
}

static double
get_hypopg_benefit(QueryDesc *queryDesc, Oid relid, AttrNumber attno)
{
    double cost_before, cost_after = 0;
    const char *relname;
    const char *attname;
    char *hypo_sql;
    int ret;

    relname = quote_identifier(get_rel_name(relid));
    attname = quote_identifier(get_attname(relid, attno, true));

    if (!relname || !attname || queryDesc->sourceText == NULL) return 0;

    cost_before = queryDesc->plannedstmt->planTree->total_cost;

    if (SPI_connect() != SPI_OK_CONNECT) return 0;

    PushActiveSnapshot(GetTransactionSnapshot());

    SPI_execute("SET hypopg.enabled = on", false, 0);

    hypo_sql = psprintf("SELECT hypopg_create_index('CREATE INDEX ON %s (%s)')", relname, attname);
    ret = SPI_execute(hypo_sql, false, 0);
    
    if (ret != SPI_OK_SELECT)
    {
        ereport(LOG, (errmsg("auto_index: hypopg_create_index failed with code %d", ret)));
    }

    CommandCounterIncrement();

    {
        List *planned_list;
        List *query_list;
        PlannedStmt *ps;
        RawStmt *raw;
        List *raw_list;
        void (*set_is_explain)(bool) = (void (*)(bool)) dlsym(RTLD_DEFAULT, "hypopg_set_is_explain");

        if (set_is_explain)
            set_is_explain(true);

        raw_list = pg_parse_query(queryDesc->sourceText);
        
        if (list_length(raw_list) > 0)
        {
            raw = (RawStmt *) linitial(raw_list);
            query_list = pg_analyze_and_rewrite_fixedparams(raw, queryDesc->sourceText, NULL, 0, NULL);
            planned_list = pg_plan_queries(query_list, queryDesc->sourceText, 0, NULL);
            
            if (list_length(planned_list) > 0)
            {
                Node *n = (Node *) linitial(planned_list);
                
                if (IsA(n, PlannedStmt))
                {
                    ps = (PlannedStmt *) n;
                    if (ps->planTree)
                        cost_after = ps->planTree->total_cost;
                }
            }
        }
        
        if (set_is_explain)
            set_is_explain(false);
    }

    ereport(LOG, (errmsg("auto_index: HypoPG Benefit Analysis - Rel: %s, Col: %s | Before: %.2f, After: %.2f, Saving: %.2f", 
                         relname, attname, cost_before, cost_after, (cost_before - cost_after))));

    SPI_execute("SELECT hypopg_reset()", false, 0);

    PopActiveSnapshot();
    SPI_finish();

    return (cost_before > cost_after) ? (cost_before - cost_after) : 0;
}

static void
auto_index_walk_planstate_tree(PlanState *planstate, QueryDesc *queryDesc)
{
    if (planstate == NULL || queryDesc == NULL)
        return;

    if (nodeTag(planstate) == T_SeqScanState)
    {
        SeqScanState *seqscan_state = (SeqScanState *) planstate;
        Relation      rel = seqscan_state->ss.ss_currentRelation;
		if(rel == NULL) return;
        
		if (rel->rd_id > 0 && rel->rd_id < FirstNormalObjectId)
		{
			ereport(LOG,
					(errmsg("auto_index: ignoring system catalog relation %u", rel->rd_id)));
			
		}else if(rel->rd_rel->relkind != RELKIND_RELATION || 
                     rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT){
			ereport(LOG,
					(errmsg("auto_index: ignoring non-permanent relation %u of kind %c",
							rel->rd_id, rel->rd_rel->relkind)));
		}else{
			auto_index_analyze_seqscan_state(seqscan_state, rel,queryDesc);
		}
    }

    auto_index_walk_planstate_tree(outerPlanState(planstate), queryDesc);
    auto_index_walk_planstate_tree(innerPlanState(planstate), queryDesc);
}


static bool
auto_index_is_equality_predicate(const OpExpr *expr)
{
	char *opname;
	bool is_equality = false;

	if (expr == NULL || !IsA(expr, OpExpr))
		return false;

	if (list_length(expr->args) != 2)
		return false;

	opname = get_opname(expr->opno);

	if (opname != NULL)
	{
		if (strcmp(opname, "=") == 0)
			is_equality = true;
		
		pfree(opname);
	}

	return is_equality;
}


static AttrNumber
auto_index_extract_var_attno(const Var *var)
{
	if (var == NULL || !IsA(var, Var))
		return InvalidAttrNumber;

	if (var->varattno <= 0)
		return InvalidAttrNumber;

	return var->varattno;
}


static Node *
auto_index_strip_relabels(Node *node)
{
    while (node != NULL && IsA(node, RelabelType))
    {
        node = (Node *) ((RelabelType *) node)->arg;
    }
    return node;
}


static Bitmapset *
auto_index_extract_equality_cols(Node *qual)
{
	Bitmapset  *result = NULL;
	ListCell   *lc;

	if (qual == NULL)
		return NULL;

	if (IsA(qual, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) qual;

		if (!auto_index_is_equality_predicate(expr))
			return NULL;

		if (list_length(expr->args) == 2)
		{
			Node	   *left = (Node *) auto_index_strip_relabels(linitial(expr->args));
			Node	   *right = (Node *) auto_index_strip_relabels(lsecond(expr->args));
			AttrNumber	left_attno = InvalidAttrNumber;
			AttrNumber	right_attno = InvalidAttrNumber;

			if (IsA(left, Var))
				left_attno = auto_index_extract_var_attno((Var *) left);
			if (IsA(right, Var))
				right_attno = auto_index_extract_var_attno((Var *) right);

		
			if (left_attno != InvalidAttrNumber && !IsA(right, Var))
			{
				result = bms_add_member(result, left_attno);
			}
			else if (right_attno != InvalidAttrNumber && !IsA(left, Var))
			{
				result = bms_add_member(result, right_attno);
			}
			else if (left_attno != InvalidAttrNumber && right_attno != InvalidAttrNumber)
			{
				result = bms_add_member(result, left_attno);
				result = bms_add_member(result, right_attno);
			}
		}
	}
	else if (IsA(qual, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) qual;

		if (expr->boolop == AND_EXPR)
		{
			foreach(lc, expr->args)
			{
				Bitmapset  *cols = auto_index_extract_equality_cols((Node *) lfirst(lc));
				if (cols)
					result = bms_union(result, cols);
				bms_free(cols);
			}
		}
	}
	else if (IsA(qual, List))
	{
		foreach(lc, (List *) qual)
		{
			Bitmapset  *cols = auto_index_extract_equality_cols((Node *) lfirst(lc));
			if (cols)
				result = bms_union(result, cols);
			bms_free(cols);
		}
	}

	return result;
}


static void
AutoIndexTrackSeqscan(Oid rel_oid, const char *rel_name, uint64 benefit, Bitmapset *indexed_attrs)
{
	TrackingKey key;
	TrackingEntry *entry;
	bool		found;
	int			attr_no = -1;

	if (!auto_index_enabled || auto_index_hash == NULL || !indexed_attrs)
		return;

	while ((attr_no = bms_next_member(indexed_attrs, attr_no)) >= 0)
	{
		memset(&key, 0, sizeof(TrackingKey));
		key.rel_oid = rel_oid;
		key.attr_no = attr_no;

		LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
		{
			entry = (TrackingEntry *) hash_search(auto_index_hash, &key,
												   HASH_ENTER, &found);

			if (!found)
			{
				entry->key = key;
				entry->scan_count = 1;
				
				entry->benefit = benefit; 
				
				entry->triggered = false;
				entry->worker_processing = false;
				entry->creation_attempts = 0;
				
				auto_index_stats->num_entries++;
				auto_index_stats->total_scans++;

				ereport(LOG,
				(errmsg("AutoIndex [NEW ENTRY]: Tracking started for Relation '%u', Column '%d'. Benefit: " UINT64_FORMAT,
						key.rel_oid, 
						key.attr_no, 
						benefit)));
			}
			else
			{
				AutoIndexUpdateEntry(entry, benefit);
				auto_index_stats->total_scans++;
				
				ereport(LOG,
				(errmsg("AutoIndex [UPDATE ENTRY]: Relation '%u', Column '%d'. Total Accumulated Benefit: " UINT64_FORMAT,
						key.rel_oid, 
						key.attr_no, 
						entry->benefit)));
			}

			if (!entry->triggered && AutoIndexCheckThreshold(entry))
			{
				entry->triggered = true;
				auto_index_stats->indices_triggered++;

				ereport(LOG,
				(errmsg("AutoIndex [TRIGGERED]: Savings (" UINT64_FORMAT ") > Build Cost for Relation '%u'. Triggering index creation!",
						entry->benefit,
						entry->key.rel_oid)));	
			}
		}
		LWLockRelease((LWLock *) auto_index_stats->lock);
	}
}

static void
AutoIndexUpdateEntry(TrackingEntry *entry, uint64 benefit)
{
	entry->scan_count++;
	entry->benefit += benefit;
}

static bool
AutoIndexCheckThreshold(TrackingEntry *entry)
{
    double build_cost;
    double total_accumulated_benefit;

    total_accumulated_benefit = (double) entry->benefit;

    build_cost = estimate_index_creation_cost(entry->key.rel_oid);

    if (total_accumulated_benefit > build_cost)
    {
        ereport(LOG, (errmsg("AutoIndex Decision: TRIGGER for Rel %u. Savings(%.2f) > BuildCost(%.2f)", 
                             entry->key.rel_oid, total_accumulated_benefit, build_cost)));
        return true;
    }

    return false;
}

PG_FUNCTION_INFO_V1(get_auto_index_stats);

PGDLLEXPORT Datum
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
		values[0] = Int32GetDatum(0);
		values[1] = Int64GetDatum(0);
		values[2] = Int64GetDatum(0);
		values[3] = Int64GetDatum(0);
	}
	else
	{
		LWLockAcquire((LWLock *) auto_index_stats->lock, LW_SHARED);
		{
			values[0] = Int32GetDatum(auto_index_stats->num_entries);
			values[1] = Int64GetDatum(auto_index_stats->total_scans);
			values[2] = Int64GetDatum(auto_index_stats->indices_triggered);
			values[3] = Int64GetDatum(auto_index_stats->indices_created);
		}
		LWLockRelease((LWLock *) auto_index_stats->lock);
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(get_auto_index_entries);

PGDLLEXPORT Datum
get_auto_index_entries(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    HASH_SEQ_STATUS status;
    TrackingEntry *entry;
    Tuplestorestate *tupstore;
    MemoryContext resultcxt;
    
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("return type must be a row type")));
    
    BlessTupleDesc(tupdesc);
    
    if (rsinfo == NULL || (rsinfo->allowedModes & SFRM_Materialize) == 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode not allowed")));
    
    resultcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    MemoryContextSwitchTo(resultcxt);
    
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    
    if (auto_index_stats == NULL || auto_index_hash == NULL || auto_index_stats->num_entries == 0)
        PG_RETURN_VOID();
    
    LWLockAcquire((LWLock *) auto_index_stats->lock, LW_SHARED);
    hash_seq_init(&status, auto_index_hash);
    
    while ((entry = (TrackingEntry *) hash_seq_search(&status)) != NULL)
    {
        Datum values[5];
        bool nulls[5] = {false, false, false, false, false};
        char *relname;
        char *attname;
        
        relname = get_rel_name(entry->key.rel_oid);
        attname = get_attname(entry->key.rel_oid, entry->key.attr_no, true);
        
        if (relname)
            values[0] = CStringGetTextDatum(relname);
        else
            nulls[0] = true;
        
        if (attname)
            values[1] = CStringGetTextDatum(attname);
        else
            nulls[1] = true;
        
        values[2] = Int64GetDatum(entry->scan_count);
        values[3] = BoolGetDatum(entry->triggered);
		values[4] = Int64GetDatum(entry->benefit);
        
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    
    LWLockRelease((LWLock *) auto_index_stats->lock);
    
    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(reset_auto_index);

PGDLLEXPORT Datum
reset_auto_index(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS status;
    TrackingEntry *entry;

    if (auto_index_stats == NULL || auto_index_hash == NULL)
        PG_RETURN_VOID();

    LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
    
    auto_index_stats->num_entries = 0;
    auto_index_stats->total_scans = 0;
    auto_index_stats->indices_triggered = 0;
    auto_index_stats->indices_created = 0;

    hash_seq_init(&status, auto_index_hash);
    while ((entry = (TrackingEntry *) hash_seq_search(&status)) != NULL)
    {
        hash_search(auto_index_hash, &entry->key, HASH_REMOVE, NULL);
    }

    LWLockRelease((LWLock *) auto_index_stats->lock);

    ereport(LOG, (errmsg("auto_index: statistics and tracking table reset")));

    PG_RETURN_VOID();
}