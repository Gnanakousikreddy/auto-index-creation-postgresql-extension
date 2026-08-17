Auto Index — Automatic Index Creation for PostgreSQL
===================================================

This is a fork of the PostgreSQL 17.9 source tree carrying **`auto_index`**, a
custom extension that turns the database into a self-optimising engine: it
watches queries as they execute, notices tables being scanned sequentially on
predictable columns, works out mathematically whether an index would pay for
itself, and — if it would — builds that index on its own from a background
process.

No DBA reads a slow query log, no one runs `EXPLAIN` by hand. The database
decides.

**Team** — Lohit Adhitya (23B0952) · Gnana Koushik (23B1000) · Akshay Karthik (23B0981) · Maneendhar (23B0969)

---

## Why

A table without the right index degrades quietly. The usual fix is reactive and
manual: someone notices slow queries, reads execution plans, decides an index is
warranted, and deploys it. That loop is slow, laborious, and easy to get wrong.

`auto_index` closes the loop inside the server. The hard part is not detecting
sequential scans — it is deciding whether an index is actually *worth it*, since
every index costs disk space and slows every future write. So instead of
guessing from scan counts alone, the extension asks the PostgreSQL planner what
the query *would have cost* if the index already existed, and only builds one
once the accumulated savings exceed the cost of building it.

---

## How it works

```
    query executes
          │
          ▼
  ExecutorEnd_hook ─────────► walk the plan tree, find SeqScan nodes
          │                            │
          │                            ▼
          │                   extract equality predicates  (WHERE col = value, ANDed)
          │                            │
          │                            ▼
          │                   cost-benefit via HypoPG:
          │                     cost_before = actual plan's total_cost
          │                     cost_after  = re-plan with a *hypothetical* index
          │                            │
          │                            ▼
          └──────────────►  shared-memory hash table, keyed (relation OID, attribute)
                                       │   accumulate  benefit += (cost_before - cost_after)
                                       ▼
                            accumulated benefit > index build cost ?
                                       │ yes → mark entry as triggered
                                       ▼
                       background worker (polls every 5s) ──► CREATE INDEX
```

### 1. Interception

`_PG_init` installs `ExecutorStart_hook` and `ExecutorEnd_hook`. The start hook
turns on row instrumentation; the end hook does the analysis, once the real plan
and its costs exist. Both hooks guard against re-entry with an `in_hook` flag —
essential, because the analysis itself runs queries through the planner and would
otherwise recurse into its own hook.

The plan tree is walked recursively for `SeqScanState` nodes. Scans are skipped
for system catalogs (OID below `FirstNormalObjectId`), and for anything that is
not a permanent, ordinary table — temporary tables, views and indexes are of no
interest.

### 2. Predicate analysis

For each surviving sequential scan, the qualifier tree is walked to find columns
compared with `=` against a non-`Var` expression. `AND`-ed conditions are
followed and each side contributes its column; `RelabelType` wrappers (the nodes
that appear around implicit type coercions) are stripped so that, for example, a
`text` column compared to a literal is still recognised. The result is a
bitmapset of candidate attribute numbers.

### 3. Cost-benefit analysis with HypoPG

This is the core of the project. To learn what an index would be worth *without
paying to build one*, the extension uses [HypoPG](https://github.com/HypoPG/hypopg),
which can register indexes that exist only in the planner's imagination:

1. `cost_before` is read straight from the executing plan —
   `queryDesc->plannedstmt->planTree->total_cost`.
2. Over SPI, `hypopg_create_index('CREATE INDEX ON rel (col)')` registers a
   virtual index.
3. `hypopg_set_is_explain(true)` is resolved at runtime with `dlsym` and called,
   which is what makes HypoPG expose the hypothetical index to the planner
   outside of an actual `EXPLAIN`.
4. The original query text (`queryDesc->sourceText`) is fed back through
   `pg_parse_query` → `pg_analyze_and_rewrite_fixedparams` → `pg_plan_queries`,
   producing a fresh plan that *can* use the imaginary index.
5. `cost_after` is read from that new plan; then `hypopg_reset()` is issued and
   the explain flag cleared, so no state leaks into subsequent queries.

The per-execution benefit is `cost_before - cost_after`, clamped at zero.

> **Note:** the vendored `contrib/hypopg` is *patched*, not stock — the exported
> `hypopg_set_is_explain()` entry point in `contrib/hypopg/hypopg.c` was added for
> this project. Build HypoPG from this tree, not from upstream.

### 4. Accumulation in shared memory

Benefits from a single query are small; the point is that queries repeat. Each
`(relation OID, attribute number)` pair gets an entry in an `HTAB` in shared
memory, so every backend on every connection contributes to the same totals:

```c
typedef struct TrackingEntry {
    TrackingKey key;               /* relation OID + attribute number */
    uint64  scan_count;            /* how many qualifying scans seen   */
    uint64  benefit;               /* Σ (cost_before - cost_after)     */
    bool    triggered;             /* threshold crossed, worker to act */
    bool    worker_processing;
    uint32  creation_attempts;
} TrackingEntry;
```

Updates are serialised with a named `LWLock` tranche; the table is created with
16 partitions and a 1000-entry ceiling.

### 5. The threshold

An index is only built when the savings it has *already* accumulated outweigh
what it will cost to construct:

```
build_cost = (blocks × seq_page_cost) + (tuples × log₂(tuples + 1) × cpu_operator_cost)

    build it  ⟺  Σ (benefit per execution)  >  build_cost
```

The two terms model the physical work of an index build: reading every page of
the table, and the comparison work of sorting every tuple.

### 6. Autonomous creation

A background worker is registered at postmaster start and connects to the
database named by `auto_index.database_name`. Every five seconds it scans the
hash table and, for any triggered entry, issues

```sql
CREATE INDEX IF NOT EXISTS idx_auto_<reloid>_<attno> ON <table> (<column>)
```

Entries whose relation has since been dropped are purged; a creation that fails
is retried up to three times; a successful one is removed from the table and
counted in `indices_created`.

---

## Using it

### SQL interface

| Function | Returns |
|---|---|
| `get_auto_index_stats()` | tracked entries, total scans, indices triggered, indices created |
| `get_auto_index_entries()` | one row per tracked column: relation, column, scans, triggered, accumulated benefit |
| `reset_auto_index()` | clears all statistics and tracking entries |

### Configuration

| GUC | Default | Scope |
|---|---|---|
| `auto_index.enabled` | `true` | `SIGHUP` — may be toggled on a running server |
| `auto_index.database_name` | `test_db` | postmaster — the database the worker connects to |

Both `auto_index` and `hypopg` must be in `shared_preload_libraries`; the
extension installs shared memory and registers its worker at startup, so it
cannot be loaded on demand.

### Build and run

Substitute your own paths for `$PGROOT` (this source tree) and `$PGDATA`.

```bash
# use the PostgreSQL you build from this tree, not the system one
export PATH=$PGROOT/install/bin:$PATH
which pg_config          # must point inside $PGROOT/install

# 1. build the patched HypoPG, then auto_index
cd $PGROOT/contrib/hypopg     && make install
cd $PGROOT/contrib/auto_index && make install

# 2. configure a cluster
export PGDATA=$PGROOT/data
initdb -D "$PGDATA"
cat >> "$PGDATA/postgresql.conf" <<'EOF'
shared_preload_libraries = 'hypopg, auto_index'
auto_index.database_name = 'test_db'
EOF
pg_ctl -D "$PGDATA" -l "$PGDATA/logfile" restart

# 3. create the database and extensions
createdb test_db
psql -d test_db -c "CREATE EXTENSION IF NOT EXISTS hypopg;
                    CREATE EXTENSION IF NOT EXISTS auto_index;"
```

Then generate some load and watch:

```bash
psql -d test_db -c "CREATE TABLE test_table (id serial primary key, val integer, data text);"
psql -d test_db -c "INSERT INTO test_table (val, data)
                    SELECT (random()*1000)::int, md5(random()::text)
                    FROM generate_series(1, 100000);"
psql -d test_db -c "ANALYZE test_table;"

for i in $(seq 1 20); do
  psql -d test_db -c "SELECT count(*) FROM test_table WHERE val = 500;" > /dev/null
done

sleep 5                                    # let the worker poll
psql -d test_db -c "\d test_table"         # the idx_auto_… index should be there
psql -d test_db -c "SELECT * FROM get_auto_index_entries();"
```

Every decision the extension makes is logged to the server log, so
`$PGDATA/logfile` shows the benefit computed per query, the threshold crossing,
and the index creation.

To re-run a test, drop the generated indexes (`WHERE indexname LIKE 'idx_auto_%'`)
and call `reset_auto_index()` to clear shared memory. A ready-made script for
both is in [`contrib/auto_index/README.md`](contrib/auto_index/README.md).

---

## Results

Tested with `SELECT * FROM t WHERE col = value` across three data distributions:

| Dataset | Outcome |
|---|---|
| 100,000 rows, random values | Large benefit per execution; threshold crossed quickly and the worker built the index |
| 1,000 rows, random values | Smaller per-query benefit; index built once enough repetitions accumulated |
| 1,000 rows, all values identical | HypoPG correctly valued the index at **zero** — no selectivity to gain — so the benefit never accumulated and no index was built |

The third case is the important one: it shows the cost model rejecting a useless
index rather than reacting to scan frequency alone.

---

## Where the code lives

```
contrib/auto_index/
├── auto_index.c          execution engine — hooks, plan-tree walk, predicate
│                         extraction, HypoPG costing, shared-memory tracking,
│                         and the three SQL-callable functions
├── auto_index_worker.c   background worker — polls, creates indexes, retries,
│                         cleans up entries for dropped relations
├── auto_index.h          shared-memory layouts (TrackingEntry, GlobalStats)
├── auto_index--1.0.sql   SQL function definitions
├── auto_index.control    extension manifest
└── README.md             step-by-step replication guide

contrib/hypopg/           vendored HypoPG 1.4.2, patched with an exported
                          hypopg_set_is_explain()

project_report/
├── report.pdf            the final report
├── checkpoint1.pdf       interim checkpoints
└── checkpoint2.pdf
```

Everything else in this tree is unmodified PostgreSQL.

---

## Limitations and notes

- **Single-column, equality-only.** Each candidate column is tracked and indexed
  on its own; range predicates (`<`, `>`, `BETWEEN`), `OR`-ed conditions, joins
  and multi-column composite indexes are outside the current scope.
- **The benefit analysis runs on the hot path.** Every qualifying sequential scan
  re-parses, re-analyses and re-plans the query inside `ExecutorEnd`. That is
  correct but not cheap; a production version would sample, cache per query
  fingerprint, or move the costing off to the worker.
- **Indexes are built with `CREATE INDEX`, not `CREATE INDEX CONCURRENTLY`.**
  The report describes the concurrent form, but the worker wraps its work in a
  transaction, and `CONCURRENTLY` cannot run inside one. As written, an index
  build takes an `ACCESS EXCLUSIVE`-style lock for its duration, so writes to the
  table wait. Making this truly non-blocking means restructuring the worker to
  run the command outside a transaction block.
- **Worker mutations are only partly locked.** The worker updates
  `triggered` / `worker_processing` / `creation_attempts` and removes hash entries
  while holding the `LWLock` only around the global counters. Under concurrent
  load this is a race worth closing before relying on the extension.
- **`benefit` is a `uint64`, while planner costs are `double`.** Fractional
  savings truncate, so very cheap queries contribute nothing.
- **One database per cluster.** The worker connects to a single database fixed at
  postmaster start by `auto_index.database_name`.
- Tracking is capped at 1000 `(relation, column)` pairs.

## AI assistance

Per the report, AI tooling was used to navigate PostgreSQL's internals — the
relationships between `QueryDesc` and `PlannedStmt`, the executor hook mechanism,
SPI usage, HypoPG's API, and the synchronisation primitives (`LWLock`, latches)
needed to manage shared memory safely. The design and implementation are the
team's own.

---

## About the underlying PostgreSQL

This repository is a fork of the PostgreSQL source distribution (17.9).
PostgreSQL is an advanced object-relational database management system
supporting an extended subset of the SQL standard, including transactions,
foreign keys, subqueries, triggers, and user-defined types and functions.

Copyright and license information is in [`COPYRIGHT`](COPYRIGHT). General
documentation is at <https://www.postgresql.org/docs/17/>, and instructions for
building from source at <https://www.postgresql.org/docs/17/installation.html>.
