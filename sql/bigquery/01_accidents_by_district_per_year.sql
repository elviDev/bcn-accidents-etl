-- Accidents per district and year (PIVOT turns years into columns).
SELECT *
FROM (
  SELECT IFNULL(nom_districte, '(unknown)') AS district, year
  FROM accidents
)
PIVOT (COUNT(*) AS y FOR year IN (2021, 2022, 2023, 2024, 2025))
ORDER BY y_2025 DESC;
