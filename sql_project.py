"""
Crimes Against Women in India – SQL Analysis Project
Dataset: Inspired by Kaggle – Crimes Against Women in India (2001–2021)
Author : Priyanka Mohapatra
Tools  : SQLite3 (Python), Pandas
"""

import sqlite3, csv, textwrap
import pandas as pd

DB = "/home/claude/crimes_women.db"
CSV = "/home/claude/crimes_against_women.csv"

# ── 1. Create DB & load data ─────────────────────────────────────────────────
conn = sqlite3.connect(DB)
cur  = conn.cursor()

cur.execute("DROP TABLE IF EXISTS crimes")
cur.execute("""
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
)
""")

with open(CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO crimes VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            row['STATE'], int(row['YEAR']),
            int(row['RAPE']), int(row['KIDNAP_ASSAULT']),
            int(row['DOWRY_DEATHS']), int(row['ASSAULT_AGAINST_WOMEN']),
            int(row['ASSAULT_AGAINST_MODESTY']), int(row['DOMESTIC_VIOLENCE']),
            int(row['WOMEN_TRAFFICKING']), int(row['TOTAL_CRIMES'])
        ))
conn.commit()

total_rows = cur.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]
print(f"✅ Loaded {total_rows} rows into crimes table\n")

# ── Helper ───────────────────────────────────────────────────────────────────
def run(title, sql, note=""):
    print("=" * 65)
    print(f"  {title}")
    if note:
        print(f"  -- {note}")
    print("=" * 65)
    df = pd.read_sql_query(sql, conn)
    print(df.to_string(index=False))
    print()
    return df

# ── 2. Basic Exploration ─────────────────────────────────────────────────────
run("Q1 · Total records & year range",
    "SELECT COUNT(*) AS total_rows, MIN(year) AS from_year, MAX(year) AS to_year FROM crimes")

run("Q2 · Crime type totals across all states & years",
    """
    SELECT
        SUM(rape)                    AS total_rape,
        SUM(kidnap_assault)          AS total_kidnapping,
        SUM(dowry_deaths)            AS total_dowry_deaths,
        SUM(assault_against_women)   AS total_assault_women,
        SUM(domestic_violence)       AS total_domestic_violence,
        SUM(women_trafficking)       AS total_trafficking,
        SUM(total_crimes)            AS grand_total
    FROM crimes
    """)

# ── 3. State-level Analysis ──────────────────────────────────────────────────
run("Q3 · Top 10 states by total crimes (all years combined)",
    """
    SELECT state,
           SUM(total_crimes) AS total_crimes_reported
    FROM crimes
    GROUP BY state
    ORDER BY total_crimes_reported DESC
    LIMIT 10
    """)

run("Q4 · Bottom 5 safest states in 2020",
    """
    SELECT state, total_crimes
    FROM crimes
    WHERE year = 2020
    ORDER BY total_crimes ASC
    LIMIT 5
    """)

run("Q5 · States where total crimes exceeded 10,000 in ANY single year",
    """
    SELECT DISTINCT state, year, total_crimes
    FROM crimes
    WHERE total_crimes > 10000
    ORDER BY total_crimes DESC
    LIMIT 10
    """)

# ── 4. Year-on-Year Trends ───────────────────────────────────────────────────
run("Q6 · Annual crime totals across India (trend over 20 years)",
    """
    SELECT year,
           SUM(total_crimes)  AS national_total,
           SUM(rape)          AS rape_total,
           SUM(domestic_violence) AS dv_total
    FROM crimes
    GROUP BY year
    ORDER BY year
    """)

run("Q7 · Year-over-year % change in national crime total",
    """
    WITH yearly AS (
        SELECT year, SUM(total_crimes) AS total
        FROM crimes GROUP BY year
    ),
    lagged AS (
        SELECT year, total,
               LAG(total) OVER (ORDER BY year) AS prev_total
        FROM yearly
    )
    SELECT year, total,
           ROUND(100.0*(total - prev_total)/prev_total, 2) AS pct_change
    FROM lagged
    WHERE prev_total IS NOT NULL
    ORDER BY year
    """, note="LAG window function – YoY % change")

# ── 5. Deep Dives ────────────────────────────────────────────────────────────
run("Q8 · Top 5 states with highest domestic violence (avg per year)",
    """
    SELECT state,
           ROUND(AVG(domestic_violence), 1) AS avg_dv_per_year,
           SUM(domestic_violence)           AS total_dv
    FROM crimes
    GROUP BY state
    ORDER BY avg_dv_per_year DESC
    LIMIT 5
    """)

run("Q9 · Rape as % of total crimes by state (2021)",
    """
    SELECT state,
           rape,
           total_crimes,
           ROUND(100.0 * rape / total_crimes, 2) AS rape_pct
    FROM crimes
    WHERE year = 2021
    ORDER BY rape_pct DESC
    LIMIT 10
    """)

run("Q10 · Dowry deaths trend in Uttar Pradesh",
    """
    SELECT year, dowry_deaths,
           SUM(dowry_deaths) OVER (ORDER BY year) AS running_total
    FROM crimes
    WHERE state = 'Uttar Pradesh'
    ORDER BY year
    """, note="Running total using SUM window function")

run("Q11 · States where trafficking INCREASED from 2015 to 2021",
    """
    SELECT s15.state,
           s15.women_trafficking AS trafficking_2015,
           s21.women_trafficking AS trafficking_2021,
           s21.women_trafficking - s15.women_trafficking AS increase
    FROM  (SELECT state, women_trafficking FROM crimes WHERE year=2015) s15
    JOIN  (SELECT state, women_trafficking FROM crimes WHERE year=2021) s21
      ON   s15.state = s21.state
    WHERE  s21.women_trafficking > s15.women_trafficking
    ORDER BY increase DESC
    """, note="Self-JOIN to compare two years")

run("Q12 · Crime distribution: which crime type forms the largest share? (national)",
    """
    SELECT
        'Rape'               AS crime_type, SUM(rape)                   AS total FROM crimes
    UNION ALL SELECT
        'Kidnapping'         ,              SUM(kidnap_assault)         FROM crimes
    UNION ALL SELECT
        'Dowry Deaths'       ,              SUM(dowry_deaths)           FROM crimes
    UNION ALL SELECT
        'Assault on Women'   ,              SUM(assault_against_women)  FROM crimes
    UNION ALL SELECT
        'Domestic Violence'  ,              SUM(domestic_violence)      FROM crimes
    UNION ALL SELECT
        'Trafficking'        ,              SUM(women_trafficking)      FROM crimes
    ORDER BY total DESC
    """, note="UNION ALL to pivot crime categories")

# ── 6. Advanced Queries ───────────────────────────────────────────────────────
run("Q13 · Top 3 states per year (2005,2010,2015,2021) – CTE + RANK",
    """
    WITH ranked AS (
        SELECT year, state, total_crimes,
               RANK() OVER (PARTITION BY year ORDER BY total_crimes DESC) AS rnk
        FROM crimes
        WHERE year IN (2005, 2010, 2015, 2021)
    )
    SELECT year, state, total_crimes, rnk
    FROM ranked
    WHERE rnk <= 3
    ORDER BY year, rnk
    """, note="CTE + RANK window function")

run("Q14 · 3-year moving average of national total crimes",
    """
    WITH yearly AS (
        SELECT year, SUM(total_crimes) AS total
        FROM crimes GROUP BY year
    )
    SELECT year, total,
           ROUND(AVG(total) OVER (ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0)
               AS moving_avg_3yr
    FROM yearly
    ORDER BY year
    """, note="Moving average with ROWS BETWEEN window frame")

run("Q15 · States with above-average domestic violence (vs national avg)",
    """
    WITH nat_avg AS (
        SELECT AVG(domestic_violence) AS avg_dv FROM crimes
    )
    SELECT c.state, c.year, c.domestic_violence,
           ROUND(n.avg_dv, 1) AS national_avg
    FROM crimes c, nat_avg n
    WHERE c.domestic_violence > n.avg_dv
      AND c.year = 2021
    ORDER BY c.domestic_violence DESC
    LIMIT 10
    """, note="Subquery / CTE with cross-reference to national average")

conn.close()
print("=" * 65)
print("  ✅ All 15 queries executed successfully")
print("=" * 65)
