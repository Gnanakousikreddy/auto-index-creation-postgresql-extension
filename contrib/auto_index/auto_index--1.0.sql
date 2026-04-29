CREATE FUNCTION get_auto_index_stats()
RETURNS TABLE (
    num_entries int,
    total_scans bigint,
    indices_triggered bigint,
    indices_created bigint
) AS 'auto_index', 'get_auto_index_stats'
LANGUAGE c STABLE PARALLEL SAFE;

COMMENT ON FUNCTION get_auto_index_stats() IS 
    'Returns current auto_index tracking statistics: number of tracked entries, total sequential scans, indices triggered, and indices created';

CREATE FUNCTION get_auto_index_entries()
RETURNS TABLE (
    relation_name text,
    column_name text,
    total_scans bigint,
    is_triggered boolean,
    benefit bigint
) AS 'auto_index', 'get_auto_index_entries'
LANGUAGE c STABLE;

COMMENT ON FUNCTION get_auto_index_entries() IS 
    'Returns detailed contents of the auto_index shared memory hash table';

CREATE FUNCTION reset_auto_index()
RETURNS void
AS 'auto_index', 'reset_auto_index'
LANGUAGE c VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION reset_auto_index() IS 
    'Clears all tracking statistics and hash table entries from shared memory';

