-- Accidents per district and year, from the continuous aggregate.
-- Postgres has no PIVOT; FILTER does the same job.
-- AT TIME ZONE keeps the year correct whatever the session time zone is.
-- coalesce(..., 0): sum() over no rows is NULL, and Postgres sorts NULL first in DESC.
SELECT
  coalesce(nom_districte, '(unknown)') AS district,
  coalesce(sum(accidents) FILTER (WHERE extract(year FROM bucket AT TIME ZONE 'Europe/Madrid') = 2021), 0)::bigint AS y_2021,
  coalesce(sum(accidents) FILTER (WHERE extract(year FROM bucket AT TIME ZONE 'Europe/Madrid') = 2022), 0)::bigint AS y_2022,
  coalesce(sum(accidents) FILTER (WHERE extract(year FROM bucket AT TIME ZONE 'Europe/Madrid') = 2023), 0)::bigint AS y_2023,
  coalesce(sum(accidents) FILTER (WHERE extract(year FROM bucket AT TIME ZONE 'Europe/Madrid') = 2024), 0)::bigint AS y_2024,
  coalesce(sum(accidents) FILTER (WHERE extract(year FROM bucket AT TIME ZONE 'Europe/Madrid') = 2025), 0)::bigint AS y_2025
FROM accidents_daily_district
GROUP BY 1
ORDER BY y_2025 DESC;
