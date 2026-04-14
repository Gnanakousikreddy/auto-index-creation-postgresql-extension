# Auto Index PostgreSQL Extension

Autonomous Index Creation for PostgreSQL - Automatically tracks sequential scans and creates indices for frequently accessed columns.

## Installation

### Build the Extension

```bash
cd postgresql/contrib/auto_index
make
make install
```

### Enable in PostgreSQL

Edit `postgresql.conf`:

```ini
shared_preload_libraries = 'auto_index'
```

Restart PostgreSQL:

```bash
pg_ctl -D $PGDATA restart
```

## GUC Parameters

Once loaded, configure via SQL:

```sql
-- Enable/disable autonomous indexing
SET auto_index.enabled = true;

-- Cost threshold to trigger indexing (planner cost units)
SET auto_index.cost_threshold = 1000;

-- Selectivity threshold for index creation (0.0 to 1.0)
SET auto_index.selectivity_threshold = 0.2;

-- Max concurrent index creation workers
SET auto_index.max_workers = 4;

-- View current settings
SHOW auto_index.enabled;
SHOW auto_index.cost_threshold;
```

## Architecture

### Phase 1-2 (Current)

Foundation layer implemented:
- GUC parameter registration
- Shared memory structures (AutoIndexStats, tracking hash table)
- Basic tracking infrastructure

### Phase 3-7 (Planned)

- Sequential scan tracking via executor hooks
- Predicate analysis and extraction
- Background worker for index creation
- SPI-based CREATE INDEX CONCURRENTLY
- Cost-based threshold calculation
- Multi-column index support
- Selectivity filtering

## Development

### Key Files

- `auto_index.c` - Main extension code
- `auto_index.control` - Extension metadata
- `auto_index--1.0.sql` - Extension SQL script
- `Makefile` - Build configuration

### Testing

Run in single-user mode:

```bash
postgres --single -D $PGDATA test
SELECT 1;
\q
```

## References

- PostgreSQL Extension Documentation: https://www.postgresql.org/docs/current/extend-extensions.html
- Shared Libraries: https://www.postgresql.org/docs/current/xfunc-c.html
- GUC Parameters: https://www.postgresql.org/docs/current/runtime-config-custom.html

## CS349 Project Team

- Lohit Adhitya (23b0952)
- GnanaKoushik (23b1000)
- AkshayKarthik (23b0981)
- Maneendhar (23b0969)

## License

Based on PostgreSQL, same license (PostgreSQL License)
