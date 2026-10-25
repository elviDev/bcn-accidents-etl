-- One month of Eixample accidents. The plan should touch only the March 2024
-- chunk, not all 60: that is chunk exclusion, the main point of a hypertable.
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*), sum(numero_victimes)
FROM accidents
WHERE accident_datetime >= '2024-03-01 00:00 Europe/Madrid'
  AND accident_datetime <  '2024-04-01 00:00 Europe/Madrid'
  AND nom_districte = 'Eixample';
