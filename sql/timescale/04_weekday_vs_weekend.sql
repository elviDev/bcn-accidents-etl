-- Weekday vs weekend, averaged per calendar day (5 weekdays to 2 weekend days).
-- generate_series builds the calendar so days with no accidents still count.
WITH daily AS (
  SELECT (bucket AT TIME ZONE 'Europe/Madrid')::date AS day,
         sum(accidents) AS accidents,
         sum(victims) AS victims,
         sum(serious_injuries + deaths) AS serious_or_fatal
  FROM accidents_daily_district
  GROUP BY 1
),
calendar AS (
  SELECT d::date AS day,
         CASE WHEN extract(isodow FROM d) IN (6, 7) THEN 'weekend' ELSE 'weekday' END AS day_type
  FROM generate_series((SELECT min(day) FROM daily), (SELECT max(day) FROM daily), INTERVAL '1 day') AS d
)
SELECT
  c.day_type,
  count(*) AS days,
  round(avg(coalesce(d.accidents, 0)), 1) AS avg_accidents_per_day,
  round(avg(coalesce(d.victims, 0)), 1) AS avg_victims_per_day,
  round(100.0 * sum(d.serious_or_fatal) / sum(d.accidents), 2) AS serious_or_fatal_per_100
FROM calendar AS c
LEFT JOIN daily AS d USING (day)
GROUP BY c.day_type
ORDER BY c.day_type;
