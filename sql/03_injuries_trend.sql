-- Yearly injury totals with year-on-year change in victims.
WITH yearly AS (
  SELECT
    year,
    COUNT(*) AS accidents,
    SUM(numero_lesionats_lleus) AS minor_injuries,
    SUM(numero_lesionats_greus) AS serious_injuries,
    SUM(numero_morts) AS deaths,
    SUM(numero_victimes) AS victims
  FROM accidents
  GROUP BY year
)
SELECT
  *,
  ROUND(100 * (victims - LAG(victims) OVER (ORDER BY year)) / LAG(victims) OVER (ORDER BY year), 1)
    AS victims_yoy_pct
FROM yearly
ORDER BY year;
