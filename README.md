# Crimes Against Women in India – SQL Analysis Project

**Author:** Priyanka Mohapatra  
**Dataset:** [Crimes Against Women in India (2001–2021) – Kaggle](https://www.kaggle.com/datasets/harigoshika/crimes-against-women-in-india-a-20-year-analysis)  
**Tools:** SQLite3 · Python · Pandas  

---

## Overview

This project performs an in-depth SQL analysis of reported crimes against women across 29 Indian states over 21 years (2001–2021). The goal is to uncover trends, identify high-risk states, and support data-driven insights relevant to women's safety — directly extending the theme of my [AI-Powered Women Safety Assistant](https://github.com/priyanka-mohapatra) project.

---

## Dataset

| Column | Description |
|---|---|
| STATE | Indian state or union territory |
| YEAR | Year of report (2001–2021) |
| RAPE | Reported rape cases |
| KIDNAP_ASSAULT | Kidnapping & assault cases |
| DOWRY_DEATHS | Dowry death cases |
| ASSAULT_AGAINST_WOMEN | Assault with intent to outrage modesty |
| ASSAULT_AGAINST_MODESTY | Insult to modesty of women |
| DOMESTIC_VIOLENCE | Cruelty by husband/relatives (IPC 498A) |
| WOMEN_TRAFFICKING | Trafficking cases |
| TOTAL_CRIMES | Sum of all crime types |

**Records:** 609 rows | **States:** 29 | **Years:** 2001–2021

---

## SQL Techniques Demonstrated

| Concept | Query |
|---|---|
| Aggregate functions (SUM, AVG, COUNT) | Q2, Q3, Q8 |
| GROUP BY + ORDER BY + LIMIT | Q3, Q4, Q9 |
| WHERE filtering | Q4, Q5 |
| Self-JOIN (compare two years) | Q11 |
| UNION ALL (unpivot columns) | Q12 |
| CTE (`WITH` clause) | Q7, Q13, Q14, Q15 |
| LAG() window function | Q7 |
| RANK() OVER PARTITION BY | Q13 |
| SUM() running total window | Q10 |
| Moving average (ROWS BETWEEN) | Q14 |
| Subquery cross-reference | Q15 |

---

## Key Findings

- **Domestic Violence** is the most prevalent crime type nationally (524,096 total cases)
- National reported crimes grew by ~58% from 2001 to 2021
- **Assam, Rajasthan, West Bengal** consistently rank as highest-crime states
- **Karnataka, Telangana, Andhra Pradesh** saw the sharpest rise in trafficking (2015–2021)
- **Haryana** had the highest proportion of rape cases as % of total crimes in 2021 (25.5%)
- Dowry deaths in Uttar Pradesh crossed a cumulative total of 4,500 over 20 years

---

## How to Run

```bash
# Clone the repo
git clone https://github.com/priyanka-mohapatra/crimes-against-women-sql

# Run the full analysis
python sql_project.py

# Or open the SQL file directly in any SQLite/MySQL/PostgreSQL client
# crimes_against_women_analysis.sql
```

---

## Files

```
├── crimes_against_women.csv          # Dataset (609 rows)
├── crimes_against_women_analysis.sql # All 15 SQL queries
├── sql_project.py                    # Python runner (SQLite3 + Pandas)
└── README.md
```

---

## Related Projects

- [AI-Powered Women Safety Assistant](https://github.com/priyanka-mohapatra) – LLM + multi-agent real-time safety app
- [Multimodal Sentiment Analysis](https://github.com/priyanka-mohapatra) – LSTM + ResNet late-fusion model
- [Gen-AI Text Analytics System](https://github.com/priyanka-mohapatra) – NLP pipeline with TF-IDF + GenAI summarization
