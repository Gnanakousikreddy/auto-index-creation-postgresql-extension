# Auto Index PostgreSQL Extension - Replication Guide

## 0. Crucial: Environment Setup
Before running ANY `make` or `psql` commands, you **must** set your PATH. This ensures you are using the version of PostgreSQL we built (18.x) and not the system default (17.x).

```bash
export PATH=/home/lelouch/postgres/install/bin:$PATH

# Verify this returns /home/lelouch/postgres/install/bin/pg_config
which pg_config

# Verify this returns "PostgreSQL 18.3" (or your local version)
pg_config --version
```

## 1. Prerequisites: Patch and Install HypoPG

```bash
cd /home/lelouch/postgres/contrib/hypopg
export PATH=/home/lelouch/postgres/install/bin:$PATH
make install
```

## 2. Build and Install auto_index

```bash
cd /home/lelouch/postgres/contrib/auto_index
export PATH=/home/lelouch/postgres/install/bin:$PATH
make install
```

## 3. Initialize and Configure PostgreSQL

```bash
export PATH=/home/lelouch/postgres/install/bin:$PATH
export PGDATA=/home/lelouch/postgres/data

# Initialize if needed
if [ ! -d "$PGDATA" ]; then
    initdb -D "$PGDATA"
    echo "port = 5433" >> "$PGDATA/postgresql.conf"
    echo "shared_preload_libraries = 'hypopg, auto_index'" >> "$PGDATA/postgresql.conf"
    echo "auto_index.database_name = 'test_db'" >> "$PGDATA/postgresql.conf"
fi

# Start server
pg_ctl -D "$PGDATA" -l "$PGDATA/logfile" restart
```

## 4. Database Setup and Verification

```bash
export PATH=/home/lelouch/postgres/install/bin:$PATH

# Create DB and Extensions
psql -h localhost -p 5433 -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'test_db'" | grep -q 1 || createdb -h localhost -p 5433 test_db
psql -h localhost -p 5433 -d test_db -c "CREATE EXTENSION IF NOT EXISTS hypopg; CREATE EXTENSION IF NOT EXISTS auto_index;"

# Populate Data (only if missing)
psql -h localhost -p 5433 -d test_db -c "DROP TABLE IF EXISTS test_table;"
psql -h localhost -p 5433 -d test_db -c "CREATE TABLE test_table (id serial primary key, val integer, data text);"
psql -h localhost -p 5433 -d test_db -c "INSERT INTO test_table (val, data) SELECT (3)::integer, md5(random()::text) FROM generate_series(1, 1000);" 
psql -h localhost -p 5433 -d test_db -c "ANALYZE test_table;"

# Trigger and Verify
for i in {1..5}; do psql -h localhost -p 5433 -d test_db -c "SELECT count(*) FROM test_table WHERE val = 500;" > /dev/null; done
sleep 5
psql -h localhost -p 5433 -d test_db -c "\d test_table"
psql -h localhost -p 5433 -d test_db -c "SELECT * FROM get_auto_index_stats();"
```

## 5. Reset for Re-testing
To run the test again, you need to remove the automatically created indexes and clear the shared memory stats:

```bash
export PATH=/home/lelouch/postgres/install/bin:$PATH

# 1. Drop all auto-generated indexes
psql -h localhost -p 5433 -d test_db -c "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT indexname FROM pg_indexes WHERE indexname LIKE 'idx_auto_%' LOOP EXECUTE 'DROP INDEX ' || quote_ident(r.indexname); END LOOP; END \$\$;"

# 2. Reset the shared memory stats and tracking table
psql -h localhost -p 5433 -d test_db -c "SELECT reset_auto_index();"

# Optional: Clear the data table if you want to repopulate
# psql -h localhost -p 5433 -d test_db -c "DROP TABLE test_table;"
```
