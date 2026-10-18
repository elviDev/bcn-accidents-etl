-- Weekday vs weekend, averaged per calendar day (there are 5 weekdays to 2 weekend days).
WITH calendar AS (
  SELECT d AS accident_date,
         IF(EXTRACT(DAYOFWEEK FROM d) IN (1, 7), 'weekend', 'weekday') AS day_type
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT MIN(accident_date) FROM accidents),
    (SELECT MAX(accident_date) FROM accidents))) AS d
),
daily AS (
  SELECT accident_date,
         COUNT(*) AS accidents,
         SUM(numero_victimes) AS victims,
         SUM(numero_lesionats_greus + numero_morts) AS serious_or_fatal
  FROM accidents
  GROUP BY accident_date
)
SELECT
  c.day_type,
  COUNT(*) AS days,
  ROUND(AVG(IFNULL(d.accidents, 0)), 1) AS avg_accidents_per_day,
  ROUND(AVG(IFNULL(d.victims, 0)), 1) AS avg_victims_per_day,
  ROUND(100 * SUM(d.serious_or_fatal) / SUM(d.accidents), 2) AS serious_or_fatal_per_100
FROM calendar AS c
LEFT JOIN daily AS d USING (accident_date)
GROUP BY c.day_type
ORDER BY c.day_type;
