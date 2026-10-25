-- Yearly injury totals with year-on-year change in victims, from the continuous aggregate.
WITH yearly AS (
  SELECT
    extract(year FROM bucket AT TIME ZONE 'Europe/Madrid')::int AS year,
    sum(accidents)::bigint AS accidents,
    sum(minor_injuries)::bigint AS minor_injuries,
    sum(serious_injuries)::bigint AS serious_injuries,
    sum(deaths)::bigint AS deaths,
    sum(victims)::bigint AS victims
  FROM accidents_daily_district
  GROUP BY 1
)
SELECT
  *,
  round(100.0 * (victims - lag(victims) OVER (ORDER BY year)) / lag(victims) OVER (ORDER BY year), 1)
    AS victims_yoy_pct
FROM yearly
ORDER BY year;
