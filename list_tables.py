import sqlite3

db_path = "data/engagement.db"
con = sqlite3.connect(db_path)

tables = con.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

print("Tables in database:")
for t in tables:
    print("-", t[0])

con.close()