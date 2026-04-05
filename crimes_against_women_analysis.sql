-- ============================================================
-- Crimes Against Women in India – SQL Analysis Project
-- Dataset : Kaggle – Crimes Against Women in India (2001–2021)
--           29 States | 21 Years | 609 Records
-- Author  : Priyanka Mohapatra
-- Tools   : SQLite3, Python, Pandas
-- GitHub  : github.com/priyanka-mohapatra
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- SECTION 0 · TABLE SCHEMA
-- ─────────────────────────────────────────────────────────────

CREATE TABLE crimes (
    state                    TEXT,
    year                     INTEGER,
    rape                     INTEGER,
    kidnap_assault           INTEGER,
    dowry_deaths             INTEGER,
    assault_against_women    INTEGER,
    assault_against_modesty  INTEGER,
    domestic_violence        INTEGER,
    women_trafficking        INTEGER,
    total_crimes             INTEGER
);


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 · BASIC EXPLORATION
-- ─────────────────────────────────────────────────────────────

-- Q1: Total records and year coverage
SELECT COUNT(*) AS total_rows,
       MIN(year) AS from_year,
       MAX(year) AS to_year
FROM crimes;


-- Q2: National totals by crime type (all years combined)
SELECT
    SUM(rape)                    AS total_rape,
    SUM(kidnap_assault)          AS total_kidnapping,
    SUM(dowry_deaths)            AS total_dowry_deaths,
    SUM(assault_against_women)   AS total_assault_women,
    SUM(domestic_violence)       AS total_domestic_violence,
    SUM(women_trafficking)       AS total_trafficking,
    SUM(total_crimes)            AS grand_total
FROM crimes;


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 · STATE-LEVEL ANALYSIS
-- ─────────────────────────────────────────────────────────────

-- Q3: Top 10 states by total crimes across all years
SELECT state,
       SUM(total_crimes) AS total_crimes_reported
FROM crimes
GROUP BY state
ORDER BY total_crimes_reported DESC
LIMIT 10;


-- Q4: 5 safest states in 2020 (lowest reported crimes)
SELECT state, total_crimes
FROM crimes
WHERE year = 2020
ORDER BY total_crimes ASC
LIMIT 5;


-- Q5: States that crossed 10,000 crimes in any single year
SELECT DISTINCT state, year, total_crimes
FROM crimes
WHERE total_crimes > 10000
ORDER BY total_crimes DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 · YEAR-ON-YEAR TREND ANALYSIS
-- ─────────────────────────────────────────────────────────────

-- Q6: National crime totals per year (rape + DV breakdown)
SELECT year,
       SUM(total_crimes)      AS national_total,
       SUM(rape)              AS rape_total,
       SUM(domestic_violence) AS domestic_violence_total
FROM crimes
GROUP BY year
ORDER BY year;


-- Q7: Year-over-year % change in national crime total
--     Uses LAG() window function
WITH yearly AS (
    SELECT year, SUM(total_crimes) AS total
    FROM crimes
    GROUP BY year
),
lagged AS (
    SELECT year, total,
           LAG(total) OVER (ORDER BY year) AS prev_total
    FROM yearly
)
SELECT year,
       total,
       ROUND(100.0 * (total - prev_total) / prev_total, 2) AS pct_change
FROM lagged
WHERE prev_total IS NOT NULL
ORDER BY year;


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 · DEEP DIVES
-- ─────────────────────────────────────────────────────────────

-- Q8: Top 5 states – highest average domestic violence per year
SELECT state,
       ROUND(AVG(domestic_violence), 1) AS avg_dv_per_year,
       SUM(domestic_violence)           AS total_dv
FROM crimes
GROUP BY state
ORDER BY avg_dv_per_year DESC
LIMIT 5;


-- Q9: Rape as % of total crimes per state in 2021
SELECT state,
       rape,
       total_crimes,
       ROUND(100.0 * rape / total_crimes, 2) AS rape_pct_of_total
FROM crimes
WHERE year = 2021
ORDER BY rape_pct_of_total DESC
LIMIT 10;


-- Q10: Dowry deaths in Uttar Pradesh with running cumulative total
--      Uses SUM() window function
SELECT year,
       dowry_deaths,
       SUM(dowry_deaths) OVER (ORDER BY year) AS cumulative_total
FROM crimes
WHERE state = 'Uttar Pradesh'
ORDER BY year;


-- Q11: States where women trafficking INCREASED from 2015 to 2021
--      Uses self-JOIN on two different years
SELECT s15.state,
       s15.women_trafficking AS trafficking_2015,
       s21.women_trafficking AS trafficking_2021,
       s21.women_trafficking - s15.women_trafficking AS increase
FROM  (SELECT state, women_trafficking FROM crimes WHERE year = 2015) s15
JOIN  (SELECT state, women_trafficking FROM crimes WHERE year = 2021) s21
  ON   s15.state = s21.state
WHERE  s21.women_trafficking > s15.women_trafficking
ORDER BY increase DESC;


-- Q12: National share of each crime type (UNION ALL to unpivot)
SELECT 'Rape'             AS crime_type, SUM(rape)                   AS total FROM crimes
UNION ALL
SELECT 'Kidnapping'                    , SUM(kidnap_assault)         FROM crimes
UNION ALL
SELECT 'Dowry Deaths'                  , SUM(dowry_deaths)           FROM crimes
UNION ALL
SELECT 'Assault on Women'              , SUM(assault_against_women)  FROM crimes
UNION ALL
SELECT 'Domestic Violence'             , SUM(domestic_violence)      FROM crimes
UNION ALL
SELECT 'Trafficking'                   , SUM(women_trafficking)      FROM crimes
ORDER BY total DESC;


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 · ADVANCED QUERIES
-- ─────────────────────────────────────────────────────────────

-- Q13: Top 3 states by crimes in selected years – CTE + RANK()
WITH ranked AS (
    SELECT year, state, total_crimes,
           RANK() OVER (PARTITION BY year ORDER BY total_crimes DESC) AS rnk
    FROM crimes
    WHERE year IN (2005, 2010, 2015, 2021)
)
SELECT year, state, total_crimes, rnk AS rank_that_year
FROM ranked
WHERE rnk <= 3
ORDER BY year, rnk;


-- Q14: 3-year moving average of national total crimes
--      Uses ROWS BETWEEN window frame
WITH yearly AS (
    SELECT year, SUM(total_crimes) AS total
    FROM crimes
    GROUP BY year
)
SELECT year,
       total,
       ROUND(
           AVG(total) OVER (ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
           0
       ) AS moving_avg_3yr
FROM yearly
ORDER BY year;


-- Q15: States with above-average domestic violence in 2021
--      Uses CTE to compute national average for comparison
WITH nat_avg AS (
    SELECT AVG(domestic_violence) AS avg_dv FROM crimes
)
SELECT c.state,
       c.year,
       c.domestic_violence,
       ROUND(n.avg_dv, 1) AS national_avg_dv
FROM crimes c, nat_avg n
WHERE c.domestic_violence > n.avg_dv
  AND c.year = 2021
ORDER BY c.domestic_violence DESC;


-- ─────────────────────────────────────────────────────────────
-- END OF FILE
-- Techniques used: GROUP BY, ORDER BY, LIMIT, WHERE, JOINS,
-- CTEs (WITH), Window Functions (LAG, RANK, SUM, AVG OVER),
-- UNION ALL, Subqueries, Aggregate Functions
-- ─────────────────────────────────────────────────────────────
