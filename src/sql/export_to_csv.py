import sqlite3
import pandas as pd

# Connect to your engagement.db
import os

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
db_path = os.path.join(base_dir, "data", "engagement.db")

con = sqlite3.connect(db_path)

# 1. Cohort-level summary (average metrics per program & intake)
cohort = pd.read_sql("""
SELECT program, intake,
       AVG(attendance_rate) AS avg_attendance,
       AVG(activity_decile) AS avg_activity,
       AVG(midterm) AS avg_midterm,
       AVG(final) AS avg_final
FROM analytic_student_term
GROUP BY program, intake
""", con)
cohort.to_csv("../../data/cohort_summary.csv", index=False)
print("✅ cohort_summary.csv exported.")

# 2. At-risk students list
at_risk = pd.read_sql("""
SELECT student_id, program, intake,
       attendance_rate, activity_decile, midterm, final,
       CASE
           WHEN attendance_rate < 0.8 OR activity_decile < 3 OR midterm < 50
           THEN 1 ELSE 0
       END AS at_risk
FROM analytic_student_term
""", con)
at_risk.to_csv("../../data/at_risk_students.csv", index=False)
print("✅ at_risk_students.csv exported.")

# 3. Engagement trends by year
trend = pd.read_sql("""
SELECT SUBSTR(term, 1, 4) AS year, program,
       AVG(attendance_rate) AS avg_attendance,
       AVG(activity_decile) AS avg_activity
FROM analytic_student_term
GROUP BY year, program
ORDER BY year
""", con)
trend.to_csv("../../data/engagement_trends.csv", index=False)
print("✅ engagement_trends.csv exported.")

con.close()
