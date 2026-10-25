-- TimescaleDB health checks: the first things to look at on a support ticket.
-- Run with:  psql "$TIMESCALE_URL" -f sql/timescale/health_checks.sql

\echo '== 1. Versions'
SELECT version() AS postgres,
       (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb') AS timescaledb;

\echo '== 2. Hypertables: chunk count and compression'
SELECT hypertable_name, num_dimensions, num_chunks, compression_enabled
FROM timescaledb_information.hypertables;

\echo '== 3. Chunk interval (too small = too many chunks, too big = poor exclusion)'
SELECT hypertable_name, column_name, time_interval
FROM timescaledb_information.dimensions;

\echo '== 4. Chunks: time range and whether compressed'
SELECT chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'accidents'
ORDER BY range_start;

\echo '== 5. Size: table, indexes, total'
SELECT pg_size_pretty(table_bytes) AS table, pg_size_pretty(index_bytes) AS indexes,
       pg_size_pretty(toast_bytes) AS toast, pg_size_pretty(total_bytes) AS total
FROM hypertable_detailed_size('accidents');

\echo '== 6. Compression savings'
SELECT total_chunks, number_compressed_chunks,
       pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes) AS after,
       round(before_compression_total_bytes::numeric / nullif(after_compression_total_bytes, 0), 1) AS ratio
FROM hypertable_compression_stats('accidents');

\echo '== 7. Background jobs: a failing policy is a common hidden problem'
SELECT j.job_id, j.proc_name, j.hypertable_name, j.schedule_interval,
       s.last_run_status, s.last_successful_finish, s.total_failures, s.next_start
FROM timescaledb_information.jobs j
LEFT JOIN timescaledb_information.job_stats s USING (job_id)
WHERE j.job_id >= 1000
ORDER BY j.job_id;

\echo '== 8. Continuous aggregates and how far they are materialized'
SELECT ca.view_name, ca.materialized_only,
       _timescaledb_functions.to_timestamp(
         _timescaledb_functions.cagg_watermark(mat_hypertable_id)) AS materialized_up_to
FROM timescaledb_information.continuous_aggregates ca
JOIN _timescaledb_catalog.continuous_agg cg ON cg.user_view_name = ca.view_name;
