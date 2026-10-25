-- Hours of the day ranked by number of accidents, with a severity rate.
-- Uses the raw hypertable: the hour is not in the daily aggregate.
SELECT
  hour,
  count(*) AS accidents,
  sum(numero_victimes) AS victims,
  sum(numero_lesionats_greus + numero_morts) AS serious_or_fatal,
  round(100.0 * sum(numero_lesionats_greus + numero_morts) / count(*), 2) AS serious_or_fatal_per_100
FROM accidents
GROUP BY hour
ORDER BY accidents DESC
LIMIT 10;
