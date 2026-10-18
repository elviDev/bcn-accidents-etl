-- Hours of the day ranked by number of accidents, with a severity rate.
SELECT
  hour,
  COUNT(*) AS accidents,
  SUM(numero_victimes) AS victims,
  SUM(numero_lesionats_greus + numero_morts) AS serious_or_fatal,
  ROUND(100 * SUM(numero_lesionats_greus + numero_morts) / COUNT(*), 2) AS serious_or_fatal_per_100
FROM accidents
GROUP BY hour
ORDER BY accidents DESC
LIMIT 10;
