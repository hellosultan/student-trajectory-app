#!/usr/bin/env python
"""
Full synthetic data loader for the Student Trajectory project.
Creates: students, attendance, lms_activity, assessments, student_events,
and rollups: attendance_term, lms_term, analytic_student_term.

Deterministic (RNG=42). Prints row counts and DB path.
"""
import os, sys, sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# -------- Config --------
RNG = np.random.default_rng(42)
DB_PATH = os.path.join("data", "engagement.db")

N_STUDENTS = 1200
TERMS = ["2024-Fall", "2025-Spring"]
PROGRAMS = ["Medicine", "Nursing", "Pharmacy"]
INTAKES = ["2024-Sep", "2025-Jan"]

FIRST_NAMES = ["Lena","Omar","Aisha","Noah","Zara","Isa","Maya","Hassan","Sofia","Adam"]
LAST_NAMES  = ["Khan","Haddad","Smith","Al Nahdi","Fernandez","Lee","Ahmed","Brown","Patel","Kim"]

def ensure_dirs():
    os.makedirs("data", exist_ok=True)

def random_name(n):
    return [f"{RNG.choice(FIRST_NAMES)} {RNG.choice(LAST_NAMES)}" for _ in range(n)]

def make_students():
    sids = np.arange(1, N_STUDENTS + 1)
    programs = RNG.choice(PROGRAMS, size=N_STUDENTS, p=[0.5, 0.3, 0.2])
    intakes = RNG.choice(INTAKES, size=N_STUDENTS, p=[0.7, 0.3])
    ages = RNG.integers(18, 45, size=N_STUDENTS)
    genders = RNG.choice(["F","M"], size=N_STUDENTS, p=[0.55, 0.45])
    names = random_name(N_STUDENTS)
    enrol_dates = np.where(np.array(intakes)=="2024-Sep",
                           pd.Timestamp("2024-09-01"),
                           pd.Timestamp("2025-01-15"))
    df = pd.DataFrame({
        "student_id": sids,
        "full_name": names,
        "program": programs,
        "intake": intakes,
        "age": ages,
        "gender": genders,
        "enrol_date": pd.to_datetime(enrol_dates).date
    })
    return df

def make_attendance(students):
    rows = []
    for term in TERMS:
        for _, r in students.iterrows():
            # realistic spread; medicine slightly higher
            base = float(np.clip(RNG.beta(8, 2) + (0.03 if r["program"]=="Medicine" else 0), 0.05, 0.99))
            for week in range(1, 13):  # 12 teaching weeks
                sessions = 5
                attended = int(RNG.binomial(sessions, base))
                rows.append([r["student_id"], term, week, sessions, attended])
    return pd.DataFrame(rows, columns=["student_id","term","week","sessions","attended"])

def make_lms_activity(students):
    rows = []
    start_date = datetime(2024, 9, 1)
    days = 26*7  # ~6 months
    for _, r in students.iterrows():
        base = max(0.8, RNG.normal(3, 1))
        mult = 1.1 if r["program"]=="Medicine" else (0.95 if r["program"]=="Nursing" else 1.0)
        for d in range(days):
            date = start_date + timedelta(days=int(d))
            clicks = max(0, int(RNG.poisson(lam=base*mult)))
            rows.append([r["student_id"], date.date().isoformat(), clicks])
    return pd.DataFrame(rows, columns=["student_id","activity_date","clicks"])

def make_assessments(students):
    rows = []
    for term in TERMS:
        for _, r in students.iterrows():
            midterm = float(np.clip(RNG.normal(62, 15), 0, 100))
            final = float(np.clip(RNG.normal(68, 14), 0, 100))
            late_submissions = int(RNG.binomial(5, 0.12))
            rows.append([r["student_id"], term, midterm, final, late_submissions])
    return pd.DataFrame(rows, columns=["student_id","term","midterm","final","late_submissions"])

def make_events(students):
    events = []
    for _, r in students.iterrows():
        enrol = pd.Timestamp(r["enrol_date"])
        events.append([r["student_id"], "Enrolled", enrol, None, r["intake"]])
        # probation signal for a subset
        if RNG.random() < 0.18:
            events.append([r["student_id"], "On Probation", enrol + pd.Timedelta(days=75), "Low attendance", "2024-Fall"])
            if RNG.random() < 0.6:
                events.append([r["student_id"], "Intervention", enrol + pd.Timedelta(days=85), "Advisor meeting", "2024-Fall"])
        # outcome at end of spring
        outcome = RNG.choice(["Progressed","Withdrew","Deferred"], p=[0.86,0.09,0.05])
        events.append([r["student_id"], outcome, pd.Timestamp("2025-06-30"), None, "2025-Spring"])
    df = pd.DataFrame(events, columns=["student_id","event_type","event_date","details","term"])
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    return df

def write_sqlite(conn, name, df, idx_cols=None):
    df.to_sql(name, conn, if_exists="replace", index=False)
    if idx_cols:
        for col in idx_cols:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{name}_{col} ON {name}({col});")

def main():
    ensure_dirs()
    print("[INFO] Building synthetic tables…")
    students = make_students()
    attendance = make_attendance(students)
    lms = make_lms_activity(students)
    assessments = make_assessments(students)
    events = make_events(students)

    con = sqlite3.connect(DB_PATH)
    try:
        print("[INFO] Writing base tables to SQLite…")
        write_sqlite(con, "students", students, ["student_id","program","intake","full_name"])
        write_sqlite(con, "attendance", attendance, ["student_id","term","week"])
        write_sqlite(con, "lms_activity", lms, ["student_id","activity_date"])
        write_sqlite(con, "assessments", assessments, ["student_id","term"])
        write_sqlite(con, "student_events", events, ["student_id","event_date","event_type"])

        # ---- Rollups for fast app queries ----
        print("[INFO] Building rollups…")
        att_term = (attendance.groupby(["student_id","term"], as_index=False)
                    .agg(total_sessions=("sessions","sum"), attended=("attended","sum")))
        write_sqlite(con, "attendance_term", att_term, ["student_id","term"])

        lms["activity_date"] = pd.to_datetime(lms["activity_date"])
        bins = {"2024-Fall": ("2024-09-01","2024-12-31"),
                "2025-Spring": ("2025-01-01","2025-06-30")}
        pieces = []
        for term,(lo,hi) in bins.items():
            m = lms["activity_date"].between(lo, hi)
            g = (lms.loc[m].groupby("student_id", as_index=False)
                 .agg(clicks=("clicks","sum")))
            g["term"] = term
            pieces.append(g)
        lms_term = pd.concat(pieces, ignore_index=True)[["student_id","term","clicks"]]
        write_sqlite(con, "lms_term", lms_term, ["student_id","term"])

        df = (students
              .merge(att_term, on="student_id")
              .merge(lms_term, on=["student_id","term"])
              .merge(assessments, on=["student_id","term"]))
        df["attendance_rate"] = df["attended"] / df["total_sessions"]
        df["activity_decile"] = pd.qcut(df["clicks"].rank(method="first"), 10, labels=False) + 1
        df["on_time_rate"] = 1 - (df["late_submissions"] / 5)
        df["at_risk"] = ((df["attendance_rate"] < 0.80) | (df["activity_decile"] <= 2)) & (df["midterm"] < 50)
        write_sqlite(con, "analytic_student_term", df,
                     ["student_id","term","program","intake"])

        # ---- Sanity prints ----
        print("[INFO] Database created:", DB_PATH)
        for t in ["students","attendance","lms_activity","assessments","student_events",
                  "attendance_term","lms_term","analytic_student_term"]:
            cur = con.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:22s}: {cur.fetchone()[0]} rows")

        return 0
    except Exception as e:
        print("[ERROR] Loader failed:", repr(e))
        return 1
    finally:
        con.close()
        print("[INFO] Connection closed.")

if __name__ == "__main__":
    sys.exit(main())
