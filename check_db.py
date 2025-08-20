import sqlite3

# Connect to the engagement database
con = sqlite3.connect("data/engagement.db")
cur = con.cursor()

# List all tables
print("📋 Tables in DB:")
for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table';"):
    print("-", row[0])

# Check student count
cur.execute("SELECT COUNT(*) FROM students;")
print("\n👩‍🎓 Students table count:", cur.fetchone()[0])

# Show sample rows from analytic_student_term
print("\n🔎 Sample rows from analytic_student_term:")
cur.execute("SELECT * FROM analytic_student_term LIMIT 5;")
rows = cur.fetchall()
for r in rows:
    print(r)

con.close()