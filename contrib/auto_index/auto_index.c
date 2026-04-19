/*-------------------------------------------------------------------------
 *
 * auto_index.c
 *    Autonomous Index Creation Infrastructure - Phase 3
 *
 * Tracks sequential scans with equality predicates and maintains statistics
 * for automatic index creation trigger decisions.
 *
 * Architecture:
 * - Shared memory: hash table indexed by (rel_oid, attr_no) pairs
 * - Executor hook: captures sequential scans at execution time
 * - Predicate analysis: extracts indexed columns from scan predicates
 * - Threshold logic: triggers index creation when cost threshold exceeded
 * - Phase 3: Actual sequential scan detection and tracking
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
#include "access/table.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "optimizer/cost.h"

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
	uint64      benefit;            /* Estimated cost benefit of indexing */
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
static bool auto_index_debug = true;

/* Hook variables */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

/* ===== Function Declarations ===== */

void _PG_init(void);
void _PG_fini(void);

static void auto_index_shmem_request(void);
static void auto_index_shmem_startup(void);
static void auto_index_executor_start(QueryDesc *queryDesc, int eflags);
static void auto_index_executor_end(QueryDesc *queryDesc);
static void auto_index_walk_planstate_tree(PlanState *planstate, QueryDesc *queryDesc);

static Bitmapset *auto_index_extract_equality_cols(Node *qual);
static AttrNumber auto_index_extract_var_attno(const Var *var);
static bool auto_index_is_equality_predicate(const OpExpr *expr);

static void AutoIndexTrackSeqscan(Oid rel_oid, const char *rel_name,
								  Cost cost, uint64 rows_processed, uint64 rows_matched, uint64 benefit,
								  Bitmapset *indexed_attrs);
static void AutoIndexUpdateEntry(TrackingEntry *entry, Cost cost,
								  uint64 rows_processed, uint64 rows_matched, uint64 benefit);
static bool AutoIndexCheckThreshold(TrackingEntry *entry);
static double AutoIndexCalculateSelectivity(const TrackingEntry *entry);
// static void AutoIndexLogStats(void);

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

	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = auto_index_executor_end;

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
		true,
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
	ExecutorEnd_hook = prev_executor_end_hook;
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
		LWLockInitialize((LWLock *) auto_index_stats->lock, lock_tranche_id);
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

static void
auto_index_executor_start(QueryDesc *queryDesc, int eflags)
{
    /* * CRITICAL: Force PostgreSQL to count the actual rows during execution.
     * We must do this before passing control to the standard executor.
     */
    if (auto_index_enabled && queryDesc != NULL)
    {
        queryDesc->instrument_options |= INSTRUMENT_ROWS;
    }

    /* Chain to previous hook or standard executor */
    if (prev_executor_start_hook)
        prev_executor_start_hook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

static void
auto_index_executor_end(QueryDesc *queryDesc)
{
    /* Walk the execution state tree to find SeqScans *after* instrumentation is finalized */
    /* We must do this AFTER standard_ExecutorEnd so instr->ntuples is populated correctly */
    if (auto_index_enabled && queryDesc != NULL && queryDesc->planstate != NULL)
    {
        auto_index_walk_planstate_tree(queryDesc->planstate, queryDesc);
    }

	/* Chain to previous hook or standard executor to finish cleanup FIRST */
    /* This is crucial: instrumentation data is finalized during ExecutorEnd */
    if (prev_executor_end_hook)
        prev_executor_end_hook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);

}


/* ===== Plan Tree Analysis Functions (Phase 3) ===== */

/*
 * auto_index_analyze_seqscan_node
 *
 * Analyzes a SeqScan node to extract indexed columns and cost information,
 * then calls AutoIndexTrackSeqscan to record the scan.
 *
 * Parameters:
 *   seqscan: SeqScan plan node (inherits from Scan, which inherits from Plan)
 *   rel: Relation object for the scanned table
 */
static void
auto_index_analyze_seqscan_state(SeqScanState *seqscan_state, Relation rel){
    
	SeqScan         *seqscan;
    Instrumentation *instr;
	WorkerInstrumentation *worker_instr;
    Bitmapset       *indexed_cols;
    Cost             scan_cost;
	Cost 			estimated_index_cost;
	Cost            benefit;
	double selectivity;	
    uint64           actual_matched_rows = 0;
    uint64           total_table_rows = 0; 
	BlockNumber       physical_pages;

    if (seqscan_state == NULL || rel == NULL)
        return;
    seqscan = (SeqScan *) seqscan_state->ss.ps.plan;
    indexed_cols = auto_index_extract_equality_cols((Node *) seqscan->scan.plan.qual);
    if (indexed_cols == NULL)
        return;
 
    instr = seqscan_state->ss.ps.instrument;
	worker_instr = seqscan_state->ss.ps.worker_instrument;

    if (instr){
        actual_matched_rows += (uint64) (instr->ntuples + instr->tuplecount);;
		if (auto_index_debug)			ereport(LOG,
					(errmsg("auto_index: relation %u has instrumentation with ntuples=%.0f, using as actual matched rows",
							rel->rd_id, instr->ntuples + instr->tuplecount)));
    }
	
	if(worker_instr){
        for (int i = 0; i < worker_instr->num_workers; i++){
            actual_matched_rows += (uint64) (worker_instr->instrument[i].ntuples + 
                                             worker_instr->instrument[i].tuplecount);
        }
		if (auto_index_debug){
			ereport(LOG,
			(errmsg("auto_index: relation %u has worker instrumentation with num_workers=%d, adding their ntuples to actual matched rows, total now: %lu",
				rel->rd_id, worker_instr->num_workers, actual_matched_rows)));
		}
	}	
	
	if (!instr && !worker_instr){
        actual_matched_rows = (uint64) seqscan->scan.plan.plan_rows;
		if (auto_index_debug)
			ereport(LOG,
					(errmsg("auto_index: instrumentation not available for relation %u, using planner estimate of matched rows: %lu",
							rel->rd_id, actual_matched_rows)));
    }

	physical_pages = RelationGetNumberOfBlocks(rel);
	// if(physical_pages < 10){
	// 	if(auto_index_debug){
	// 		ereport(LOG,
	// 				(errmsg("auto_index: relation %u has only %u physical pages, likely very small, skipping tracking",
	// 						rel->rd_id, physical_pages)));
	// 	}
	// 	bms_free(indexed_cols);
	// 	return;
	// }
	
    if (rel->rd_rel->reltuples > 0){
		total_table_rows = (uint64) rel->rd_rel->reltuples;
		if(auto_index_debug)
			ereport(LOG,
					(errmsg("auto_index: relation %u has reltuples=%.0f, using as total rows processed",
							rel->rd_id, rel->rd_rel->reltuples)));
	}else{
		BlockNumber estimated_pages;
		double estimated_tuples;
		double allvisfrac;
		int32 attr_width[MaxHeapAttributeNumber];

		table_block_relation_estimate_size(
			rel, 
			attr_width, 
			&estimated_pages, 
			&estimated_tuples, 
			&allvisfrac, 
			SizeofHeapTupleHeader + sizeof(ItemIdData),
			BLCKSZ - SizeOfPageHeaderData
		);
		total_table_rows = (uint64) estimated_tuples;
		if(total_table_rows ==0) total_table_rows = 1;
		if(auto_index_debug){
			ereport(LOG,
					(errmsg("auto_index: relation %u has no reltuples estimate, using table_block_relation_estimate_size to estimate total rows processed as %.0f",
							rel->rd_id, estimated_tuples)));
		}
	}

    if (actual_matched_rows > total_table_rows){
        actual_matched_rows = total_table_rows;
	}

	selectivity = (double)actual_matched_rows / (double) total_table_rows;
	if(selectivity > 0.2){
		if (auto_index_debug){
            ereport(LOG,
                    (errmsg("auto_index: query too broad (selectivity %.2f) for relation %u, skipping.",
                            selectivity, rel->rd_id)));
		}
        bms_free(indexed_cols);
        return;
	}

	if(rel->rd_rel->reltuples > 0){
		scan_cost = seqscan->scan.plan.total_cost - seqscan->scan.plan.startup_cost;
	}else{
		scan_cost = ((double) physical_pages * DEFAULT_SEQ_PAGE_COST) + 
                    ((double) total_table_rows * (DEFAULT_CPU_TUPLE_COST + DEFAULT_CPU_OPERATOR_COST));
		if (auto_index_debug){
            ereport(LOG,
                    (errmsg("auto_index: missing stats on relation %u. Recalculated cost to %.2f",
                            rel->rd_id, scan_cost)));
		}
	}

	estimated_index_cost = ((double) actual_matched_rows * DEFAULT_RANDOM_PAGE_COST) + 
                           ((double) actual_matched_rows * DEFAULT_CPU_INDEX_TUPLE_COST);
	benefit = scan_cost - estimated_index_cost;
	if(benefit <= 0){
		if(auto_index_debug){
			ereport(LOG,
					(errmsg("auto_index: estimated index cost %.2f exceeds scan cost %.2f for relation %u, skipping.",
							estimated_index_cost, scan_cost, rel->rd_id)));
		}
		bms_free(indexed_cols);
		return;
	}

    if (auto_index_debug){
        ereport(LOG,
                (errmsg("auto_index: relation %u (%s) | Selectivity: %.4f | Seq Cost: %.2f | Idx Cost: %.2f | Benefit: %.2f",
                        rel->rd_id, RelationGetRelationName(rel), selectivity, 
                        scan_cost, estimated_index_cost, benefit)));
	}
    AutoIndexTrackSeqscan(rel->rd_id, RelationGetRelationName(rel),
                         scan_cost, total_table_rows, actual_matched_rows, benefit, indexed_cols);
    bms_free(indexed_cols);
}

/*
 * auto_index_walk_plan_tree
 *
 * Recursively walks a plan tree to find all SeqScan nodes and analyze them.
 * Uses a simple recursive descent approach (not using walk_plan_tree to avoid
 * adding complexity).
 *
 * Parameters:
 *   plan: Current plan node to analyze
 *   queryDesc: Query descriptor for relation lookups
 */
static void
auto_index_walk_planstate_tree(PlanState *planstate, QueryDesc *queryDesc)
{
    if (planstate == NULL || queryDesc == NULL)
        return;

    /* Check if this node is an actively executing sequential scan */
    if (nodeTag(planstate) == T_SeqScanState)
    {
        SeqScanState *seqscan_state = (SeqScanState *) planstate;
        Relation      rel = seqscan_state->ss.ss_currentRelation;
		if(rel == NULL) return;
        
		if (rel->rd_id > 0 && rel->rd_id < FirstNormalObjectId)
		{
			if (auto_index_debug)
			{
				ereport(LOG,
						(errmsg("auto_index: ignoring system catalog relation %u", rel->rd_id)));
			}
		}else if(rel->rd_rel->relkind != RELKIND_RELATION || 
                     rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT){
			if (auto_index_debug)			{
				ereport(LOG,
						(errmsg("auto_index: ignoring non-permanent relation %u of kind %c",
								rel->rd_id, rel->rd_rel->relkind)));
			}
		}else{
			auto_index_analyze_seqscan_state(seqscan_state, rel);
		}
    }

    /* * Recursively process left and right subtrees 
     * Note: PlanState trees use outerPlanState and innerPlanState macros
     */
    auto_index_walk_planstate_tree(outerPlanState(planstate), queryDesc);
    auto_index_walk_planstate_tree(innerPlanState(planstate), queryDesc);
    
    /* * SubPlans and InitPlans are skipped for now, similar to your original logic.
     * They require iterating over planstate->initPlan and planstate->subPlan.
     */
}

/* ===== Predicate Extraction Functions (Phase 3) ===== */

/*
 * auto_index_is_equality_predicate
 *
 * Safely determines if an operator expression is strictly an equality ('=') predicate.
 * Checks the argument count and queries the system catalog for the operator name.
 */
static bool
auto_index_is_equality_predicate(const OpExpr *expr)
{
	char *opname;
	bool is_equality = false;

	/* Sanity check */
	if (expr == NULL || !IsA(expr, OpExpr))
		return false;

	/* Binary operators must have exactly 2 arguments */
	if (list_length(expr->args) != 2)
		return false;

	/* * Lookup the operator's actual name in the pg_operator catalog
	 * using the operator's OID (opno).
	 */
	opname = get_opname(expr->opno);

	if (opname != NULL)
	{
		/* Check if the operator symbol is exactly '=' */
		if (strcmp(opname, "=") == 0)
			is_equality = true;
		
		/* get_opname allocates memory, so we must free it to prevent leaks */
		pfree(opname);
	}

	return is_equality;
}

/*
 * auto_index_extract_var_attno
 *
 * Extracts the attribute number from a Var node.
 * Returns InvalidAttrNumber if not a Var or invalid attribute.
 *
 * Parameters:
 *   var: Var node to extract from
 *
 * Returns:
 *   Attribute number (1-based) or InvalidAttrNumber
 */
static AttrNumber
auto_index_extract_var_attno(const Var *var)
{
	if (var == NULL || !IsA(var, Var))
		return InvalidAttrNumber;

	/* Only track user columns, not system columns or whole-row references */
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

/*
 * auto_index_extract_equality_cols
 *
 * Recursively traverses a qual (WHERE clause) and extracts columns used
 * in equality predicates. Returns a Bitmapset of attribute numbers.
 *
 * Handles:
 * - Simple OpExpr: col = value
 * - BoolExpr (AND): recursively processes each clause
 * - Ignores OR expressions and other operators
 *
 * Parameters:
 * qual: Query qualification (WHERE clause) node tree
 *
 * Returns:
 * Bitmapset of attribute numbers involved in equality predicates
 * NULL if no equality predicates found
 */
// check
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

		/* Check if this is an equality predicate */
		if (!auto_index_is_equality_predicate(expr))
			return NULL;

		/*
		 * For equality predicates, extract the column reference.
		 * Typical form: col = const or const = col
		 */
		if (list_length(expr->args) == 2)
		{
			/* * Extract the left and right arguments, stripping away any RelabelType 
			 * nodes that were added for implicit casting (e.g., varchar to text).
			 */
			Node	   *left = (Node *) auto_index_strip_relabels(linitial(expr->args));
			Node	   *right = (Node *) auto_index_strip_relabels(lsecond(expr->args));
			AttrNumber	left_attno = InvalidAttrNumber;
			AttrNumber	right_attno = InvalidAttrNumber;

			/* Try to extract attribute numbers */
			if (IsA(left, Var))
				left_attno = auto_index_extract_var_attno((Var *) left);
			if (IsA(right, Var))
				right_attno = auto_index_extract_var_attno((Var *) right);

			/*
			 * Include in result if one side is a Var and the other is a Const
			 * or expression. This handles: col = const, const = col, col = col
			 */
			if (left_attno != InvalidAttrNumber && !IsA(right, Var))
			{
				/* Case 1: Column on the left, Constant/Expression on the right */
				result = bms_add_member(result, left_attno);
			}
			else if (right_attno != InvalidAttrNumber && !IsA(left, Var))
			{
				/* Case 2: Constant/Expression on the left, Column on the right */
				result = bms_add_member(result, right_attno);
			}
			else if (left_attno != InvalidAttrNumber && right_attno != InvalidAttrNumber)
			{
				/* Case 3: Column on the left AND Column on the right */
				result = bms_add_member(result, left_attno);
				result = bms_add_member(result, right_attno);
			}
		}
	}
	else if (IsA(qual, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) qual;

		/*
		 * For AND clauses, recursively process each clause.
		 * For OR clauses, skip (can't reliably track selectivity).
		 */
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
		/* Process list of clauses (conjunction) */
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

/* ===== Executor Hook Implementation ===== */

// static void
// auto_index_executor_start(QueryDesc *queryDesc, int eflags)
// {
// 	/* Call previous hook first */
// 	if (prev_executor_start_hook)
// 		prev_executor_start_hook(queryDesc, eflags);
// 	else
// 		standard_ExecutorStart(queryDesc, eflags);

// 	/* Phase 3: Analyze query plan for sequential scans with equality predicates
// 	 * This hook is called once per query, allowing us to extract plan information
// 	 * before execution begins. We analyze the plan tree to identify SeqScan nodes
// 	 * and extract their predicates for tracking purposes.
// 	 * 
// 	 * Note: This phase tracks planned scans. Actual row counting during execution
// 	 * happens separately and will be added in a future phase. This phase focuses
// 	 * on plan analysis and cost extraction.
// 	 */
// 	if (!auto_index_enabled || queryDesc == NULL || queryDesc->plannedstmt == NULL)
// 		return;

// 	/* Walk the plan tree to find and analyze sequential scans */
// 	auto_index_walk_plan_tree(queryDesc->plannedstmt->planTree, queryDesc);
// }

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
					  Cost cost, uint64 rows_processed, uint64 rows_matched, uint64 benefit,
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
		LWLockAcquire((LWLock *) auto_index_stats->lock, LW_EXCLUSIVE);
		{
			entry = (TrackingEntry *) hash_search(auto_index_hash, &key,
												   HASH_ENTER, &found);

			if (!found){
				/* Initialize new entry */
				entry->key = key;
				entry->scan_count = 1;
				entry->accumulated_cost = cost;
				entry->rows_processed = rows_processed;
				entry->rows_matched = rows_matched;
				entry->benefit = benefit;
				entry->triggered = false;
				auto_index_stats->num_entries++;
				auto_index_stats->total_scans++;

				if(auto_index_debug){
					ereport(LOG,
					(errmsg("AutoIndex [NEW ENTRY]: Tracking started for Relation '%d', Column '%d'. Cost: %.2f, Rows Processed: %lu, Rows Matched: %lu, Benefit: %lu",
							key.rel_oid, 
							key.attr_no, 
							cost,
							rows_processed,
							rows_matched,
							benefit)));
				}
			}else{
				/* Update tracking statistics */
				AutoIndexUpdateEntry(entry, cost, rows_processed, rows_matched, benefit);
				auto_index_stats->total_scans++;
				if(auto_index_debug){
					ereport(LOG,
					(errmsg("AutoIndex [UPDATE ENTRY]: Updated tracking for Relation '%d', Column '%d'. Total Cost: %lu, Total Scans: %lu, Rows Processed: %lu, Rows Matched: %lu, Benefit: %lu",
							key.rel_oid, 
							key.attr_no, 
							entry->accumulated_cost,
							entry->scan_count,
							entry->rows_processed,
							entry->rows_matched,
							entry->benefit)));
				}
			}

			/* Check if threshold exceeded */
			if (!entry->triggered && AutoIndexCheckThreshold(entry))
			{
				entry->triggered = true;
				auto_index_stats->indices_triggered++;

				if (auto_index_debug)
					ereport(LOG,
					(errmsg("AutoIndex [THRESHOLD REACHED]: Relation '%d', Column '%d' exceeded cost threshold (%lu >= %d). Triggering Index Creation!",
							entry->key.rel_oid, 
							entry->key.attr_no, 
							entry->accumulated_cost,
							auto_index_cost_threshold)));	
			}
		}
		LWLockRelease((LWLock *) auto_index_stats->lock);
	}
}

/*
 * AutoIndexUpdateEntry
 *
 * Updates tracking entry with new scan statistics
 */
static void
AutoIndexUpdateEntry(TrackingEntry *entry, Cost cost,
					  uint64 rows_processed, uint64 rows_matched, uint64 benefit)
{
	entry->scan_count++;
	entry->accumulated_cost += (uint64) cost;
	entry->rows_processed += rows_processed;
	entry->rows_matched += rows_matched;
	entry->benefit += benefit;
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
	if (entry->benefit <= (uint64) auto_index_cost_threshold)
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
// static void
// AutoIndexLogStats(void)
// {
// 	if (!auto_index_enabled || auto_index_stats == NULL)
// 		return;

// 	ereport(LOG,
// 			(errmsg("auto_index stats: entries=%d, scans=%lu, triggered=%lu, created=%lu",
// 					auto_index_stats->num_entries,
// 					auto_index_stats->total_scans,
// 					auto_index_stats->indices_triggered,
// 					auto_index_stats->indices_created)));
// }

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

/*
 * get_auto_index_entries() returns SETOF record
 * Returns all tracking entries currently in the shared memory hash table.
 * 
 * Uses the simple approach without tuplestore
 */
PG_FUNCTION_INFO_V1(get_auto_index_entries);

Datum
get_auto_index_entries(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    HASH_SEQ_STATUS status;
    TrackingEntry *entry;
    Tuplestorestate *tupstore;
    MemoryContext resultcxt;
    
    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("return type must be a row type")));
    
    BlessTupleDesc(tupdesc);
    
    /* Verify materialize mode allowed */
    if (rsinfo == NULL || (rsinfo->allowedModes & SFRM_Materialize) == 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode not allowed")));
    
    /* Create tuplestore in function's per-query context */
    resultcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    MemoryContextSwitchTo(resultcxt);
    
    /* Set return info */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    
    /* If no data, return empty result */
    if (auto_index_stats == NULL || auto_index_hash == NULL || auto_index_stats->num_entries == 0)
        PG_RETURN_VOID();
    
    /* Acquire lock and iterate */
    LWLockAcquire((LWLock *) auto_index_stats->lock, LW_SHARED);
    hash_seq_init(&status, auto_index_hash);
    
    while ((entry = (TrackingEntry *) hash_seq_search(&status)) != NULL)
    {
        Datum values[6];
        bool nulls[6] = {false, false, false, false, false, false};
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
        values[3] = Float8GetDatum(entry->accumulated_cost);
        values[4] = BoolGetDatum(entry->triggered);
		values[5] = Int64GetDatum(entry->benefit);
        
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    
    /* Release lock */
    LWLockRelease((LWLock *) auto_index_stats->lock);
    
    PG_RETURN_VOID();
}