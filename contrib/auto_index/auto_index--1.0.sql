/* auto_index--1.0.sql */

-- SQL functions and objects for auto_index extension

-- Create the monitoring function to view current statistics
CREATE FUNCTION get_auto_index_stats()
RETURNS TABLE (
    num_entries int,
    total_scans bigint,
    indices_triggered bigint,
    indices_created bigint
) AS 'auto_index', 'get_auto_index_stats'
LANGUAGE c STABLE PARALLEL SAFE;

-- Informational comment
COMMENT ON FUNCTION get_auto_index_stats() IS 
    'Returns current auto_index tracking statistics: number of tracked entries, total sequential scans, indices triggered, and indices created';

-- Debug: just return count
-- CREATE FUNCTION get_auto_index_count()
-- RETURNS int
-- AS 'auto_index', 'get_auto_index_count'
-- LANGUAGE c STABLE;

-- View detailed tracking entries in the hash table
CREATE FUNCTION get_auto_index_entries()
RETURNS SETOF record
AS 'auto_index', 'get_auto_index_entries'
LANGUAGE c STABLE
ROWS 100;

COMMENT ON FUNCTION get_auto_index_entries() IS 
    'Returns detailed contents of the auto_index shared memory hash table';

-- GUC parameters registered by the extension:
--   auto_index.enabled (bool) - Enable/disable autonomous indexing
--   auto_index.cost_threshold (int) - Cost threshold for triggering index creation
--   auto_index.selectivity_threshold (real) - Selectivity threshold for indexed predicates
--   auto_index.max_workers (int) - Maximum concurrent index creation workers
--   auto_index.debug (bool) - Enable debug logging

