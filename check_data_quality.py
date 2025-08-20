import sqlite3
import pandas as pd

db_path = "data/engagement.db"
con = sqlite3.connect(db_path)

# Load analytic_student_term for checks
df = pd.read_sql_query("SELECT * FROM analytic_student_term", con)

report = {}

# Basic info
report["rows"] = len(df)
report["columns"] = list(df.columns)

# Missing values
missing = df.isnull().sum()
report["missing_values"] = missing[missing > 0].to_dict()

# Duplicates
report["duplicates"] = int(df.duplicated().sum())

# Anomaly checks
anomalies = {}

if "avg_grade" in df.columns:
    bad_grades = df[(df["avg_grade"] < 0) | (df["avg_grade"] > 100)]
    anomalies["bad_grades"] = len(bad_grades)

if "attendance_ratio" in df.columns:
    bad_attendance = df[(df["attendance_ratio"] < 0) | (df["attendance_ratio"] > 1)]
    anomalies["bad_attendance"] = len(bad_attendance)

if "engagement_score" in df.columns:
    bad_engagement = df[(df["engagement_score"] < 0) | (df["engagement_score"] > 1)]
    anomalies["bad_engagement"] = len(bad_engagement)

if "term" in df.columns:
    weird_terms = df[~df["term"].str.contains("20")]
    anomalies["weird_terms"] = len(weird_terms)

report["anomalies"] = anomalies

# Save as CSV
df.head(100).to_csv("data/sample_data_preview.csv", index=False)  # preview of 100 rows

# Save report to Markdown
with open("data/data_quality_report.md", "w") as f:
    f.write("# Data Quality Report\n\n")
    f.write(f"**Total Rows:** {report['rows']}\n\n")
    f.write(f"**Columns:** {report['columns']}\n\n")

    f.write("## Missing Values\n")
    if report["missing_values"]:
        for k, v in report["missing_values"].items():
            f.write(f"- {k}: {v}\n")
    else:
        f.write("✅ No missing values found\n")
    f.write("\n")

    f.write(f"## Duplicates\n- {report['duplicates']} duplicate rows\n\n")

    f.write("## Anomalies\n")
    if anomalies:
        for k, v in anomalies.items():
            f.write(f"- {k}: {v}\n")
    else:
        f.write("✅ No anomalies detected\n")

con.close()

print("✅ Data quality check complete!")
print("Report saved to: data/data_quality_report.md")
print("Sample data preview saved to: data/sample_data_preview.csv")