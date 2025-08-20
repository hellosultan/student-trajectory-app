#!/usr/bin/env python
"""
Full synthetic data loader (multi-year, per-program durations)
- Academic year runs Sep -> Aug:
    Fall  = Sep–Dec
    Spring= Jan–Jun
- Programme durations:
    Medicine=6y, Nursing=4y, Pharmacy=5y (configurable)
- Each student receives ALL terms of their programme from intake onward.
- Tables: students, attendance, lms_activity, assessments, student_events
- Rollups: attendance_term, lms_term, analytic_student_term
"""

import os, sys, sqlite3, argparse
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

PROGRAM_DURATION = {
    "Medicine": 6,
    "Nursing": 4,
    "Pharmacy": 5,
}

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--students", type=int, default=12000, help="Number of students to generate")
    p.add_argument("--year_start", type=int, default=2012, help="First academic year (Fall)")
    p.add_argument("--year_end", type=int, default=2025, help="Last academic year bound (Spring ends no later than this if possible)")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument("--db", type=str, default=os.path.join("data", "engagement.db"), help="SQLite DB path")
    return p.parse_args()

# ------------- Helpers --------------
def term_dates(term: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return (start_date, end_date) for 'YYYY-Fall' or 'YYYY-Spring'."""
    y, sem = term.split("-")
    y = int(y)
    if sem == "Fall":
        return pd.Timestamp(y, 9, 1), pd.Timestamp(y, 12, 31)
    else:  # Spring
        return pd.Timestamp(y, 1, 1), pd.Timestamp(y, 6, 30)

def all_terms_for_program(intake: str, program: str, duration_map: dict) -> list[str]:
    """
    Given intake like '2018-Sep' or '2019-Jan', return all academic terms
    (Fall/Spring pairs) for the programme duration.
    """
    start_year = int(intake.split("-")[0])
    # Jan intake's first academic year Spring happens in 'start_year'
    if intake.endswith("Jan"):
        first_fall_year = start_year - 1
    else:  # Sep intake
        first_fall_year = start_year

    n_years = int(duration_map.get(program, 4))
    terms = []
    for k in range(n_years):
        fall_y = first_fall_year + k
        spring_y = fall_y + 1
        terms.append(f"{fall_y}-Fall")
        terms.append(f"{spring_y}-Spring")
    return terms

def unique_names(n: int, rng: np.random.Generator) -> list[str]:
    """Large, readable synthetic name space to minimize duplicates."""
    first_syl = ["A", "Be", "Ca", "Da", "El", "Fa", "Gi", "Ha", "I", "Ja", "Ka", "La", "Ma", "Na", "O", "Pa", "Qi", "Ra", "Sa", "Ta", "Uma", "Va", "Wa", "Xa", "Ya", "Za"]
    last_syl1 = ["Al", "Ben", "Car", "Dia", "Fern", "Gon", "Ham", "Ivan", "Jun", "Kim", "Lee", "Mor", "Nov", "Omar", "Park", "Quan", "Ross", "Sing", "Tan", "Umar", "Val", "Wang", "Xu", "Yam", "Zar"]
    last_syl2 = ["son", "s", "ez", "ov", "ski", "sen", "Li", "chi", "yan", "man", "ford", "elli", "dell", "berg", "wala", "ova", "ian", "aro", "etti", "ato", "ino"]
    middle = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    combos = []
    for a in first_syl:
        for b in last_syl1:
            for c in last_syl2:
                combos.append((a, b + c))
    rng.shuffle(combos)

    out, used = [], set()
    i = 0
    while len(out) < n:
        fi, la = combos[i % len(combos)]
        # readable first name like 'Ana', 'Bena', 'Cana', …
        first = f"{fi}na"
        mid = rng.choice(middle)
        candidate = f"{first} {la} {mid}."
        if candidate not in used:
            out.append(candidate)
            used.add(candidate)
        i += 1
        if i > len(combos) * 2:  # last resort
            candidate = f"{first} {la} {mid}.{rng.integers(1,9999)}"
            if candidate not in used:
                out.append(candidate)
                used.add(candidate)
    return out[:n]

def write_sqlite(conn, name, df, idx_cols=None):
    df.to_sql(name, conn, if_exists="replace", index=False)
    if idx_cols:
        for col in idx_cols:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{name}_{col} ON {name}({col});")

# ------------- Builders --------------
def make_students(n_students: int, years: tuple[int,int], rng: np.random.Generator):
    y0, y1 = years
    durations = PROGRAM_DURATION

    # To keep all terms within the requested window, choose intake years
    # so that the last Spring roughly ends by year_end.
    max_dur = max(durations.values())
    earliest_intake = y0
    latest_intake = max(y0, y1 - max_dur)  # e.g., 2025 - 6 = 2019 for Medicine

    programs = rng.choice(["Medicine","Nursing","Pharmacy"], size=n_students, p=[0.45,0.35,0.20])
    intake_years = rng.integers(earliest_intake, latest_intake + 1, size=n_students)
    intakes_mnth = rng.choice(["Sep","Jan"], size=n_students, p=[0.7,0.3])
    intake_str = [f"{y}-Sep" if m=="Sep" else f"{y}-Jan" for y,m in zip(intake_years, intakes_mnth)]

    names = unique_names(n_students, rng)
    ages = rng.integers(18, 45, size=n_students)
    genders = rng.choice(["F","M"], size=n_students, p=[0.55,0.45])

    # enrol_date aligned to academic year
    enrol_dates = [pd.Timestamp(y, 9, 1) if m=="Sep" else pd.Timestamp(y, 1, 15)
               for y,m in zip(intake_years, intakes_mnth)]

    return pd.DataFrame({
        "student_id": np.arange(1, n_students+1),
        "full_name": names,
        "program": programs,
        "intake": intake_str,
        "age": ages,
        "gender": genders,
        "enrol_date": enrol_dates
    })

def make_attendance(students: pd.DataFrame, rng: np.random.Generator):
    rows = []
    for _, r in students.iterrows():
        terms = all_terms_for_program(r["intake"], r["program"], PROGRAM_DURATION)
        for term in terms:
            base = float(np.clip(rng.beta(8,2) + (0.03 if r["program"]=="Medicine" else 0.0), 0.05, 0.99))
            for week in range(1, 13):  # 12 teaching weeks
                sessions = 5
                attended = int(rng.binomial(sessions, base))
                rows.append([r["student_id"], term, week, sessions, attended])
    return pd.DataFrame(rows, columns=["student_id","term","week","sessions","attended"])

def make_lms_activity(students: pd.DataFrame, rng: np.random.Generator):
    rows = []
    for _, r in students.iterrows():
        terms = all_terms_for_program(r["intake"], r["program"], PROGRAM_DURATION)
        for term in terms:
            d0, d1 = term_dates(term)
            days = (d1 - d0).days + 1
            base = max(0.8, rng.normal(3, 1))
            mult = 1.1 if r["program"]=="Medicine" else (0.95 if r["program"]=="Nursing" else 1.0)
            for i in range(days):
                dt = d0 + timedelta(days=i)
                clicks = max(0, int(rng.poisson(lam=base*mult)))
                rows.append([r["student_id"], dt.isoformat(), clicks])
    return pd.DataFrame(rows, columns=["student_id","activity_date","clicks"])

def make_assessments(students: pd.DataFrame, rng: np.random.Generator):
    rows = []
    for _, r in students.iterrows():
        terms = all_terms_for_program(r["intake"], r["program"], PROGRAM_DURATION)
        for term in terms:
            midterm = float(np.clip(rng.normal(62, 15), 0, 100))
            final = float(np.clip(rng.normal(68, 14), 0, 100))
            late_submissions = int(rng.binomial(5, 0.12))
            rows.append([r["student_id"], term, midterm, final, late_submissions])
    return pd.DataFrame(rows, columns=["student_id","term","midterm","final","late_submissions"])

def make_events(students: pd.DataFrame, rng: np.random.Generator):
    rows = []
    for _, r in students.iterrows():
        enrol = pd.to_datetime(r["enrol_date"])
        terms = all_terms_for_program(r["intake"], r["program"], PROGRAM_DURATION)
        rows.append([r["student_id"], "Enrolled", enrol, None, terms[0]])
        # probation in an early term
        if rng.random() < 0.18:
            tprob = terms[min(1, len(terms)-1)]
            rows.append([r["student_id"], "On Probation", enrol + timedelta(days=75), "Low attendance", tprob])
            if rng.random() < 0.6:
                rows.append([r["student_id"], "Intervention", enrol + timedelta(days=85), "Advisor meeting", tprob])
        # outcome at last spring
        final_term = terms[-1]  # always a Spring term per builder
        end_spring = term_dates(final_term)[1]
        outcome = rng.choice(["Graduated","Withdrew","Deferred"], p=[0.85, 0.10, 0.05])
        rows.append([r["student_id"], outcome, end_spring, None, final_term])
    df = pd.DataFrame(rows, columns=["student_id","event_type","event_date","details","term"])
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df

# ------------- Main --------------
def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    print(f"[INFO] Programme durations: {PROGRAM_DURATION}")
    print(f"[INFO] Window: {args.year_start}..{args.year_end}  Students: {args.students}  Seed: {args.seed}")

    students = make_students(args.students, (args.year_start, args.year_end), rng)
    attendance = make_attendance(students, rng)
    lms = make_lms_activity(students, rng)
    assessments = make_assessments(students, rng)
    events = make_events(students, rng)

    con = sqlite3.connect(args.db)
    try:
        print("[INFO] Writing base tables…")
        write_sqlite(con, "students", students, ["student_id","program","intake","full_name"])
        write_sqlite(con, "attendance", attendance, ["student_id","term","week"])
        write_sqlite(con, "lms_activity", lms, ["student_id","activity_date"])
        write_sqlite(con, "assessments", assessments, ["student_id","term"])
        write_sqlite(con, "student_events", events, ["student_id","event_date","event_type"])

        # -------- Rollups --------
        print("[INFO] Building rollups…")
        att_term = (attendance.groupby(["student_id","term"], as_index=False)
                    .agg(total_sessions=("sessions","sum"),
                         attended=("attended","sum")))
        write_sqlite(con, "attendance_term", att_term, ["student_id","term"])

        lms["activity_date"] = pd.to_datetime(lms["activity_date"])
        pieces = []
        for term in pd.unique(att_term["term"]):
            lo, hi = term_dates(term)
            m = lms["activity_date"].between(lo, hi)
            g = (lms.loc[m].groupby("student_id", as_index=False)
                 .agg(clicks=("clicks","sum")))
            g["term"] = term
            pieces.append(g)
        lms_term = pd.concat(pieces, ignore_index=True)[["student_id","term","clicks"]]
        write_sqlite(con, "lms_term", lms_term, ["student_id","term"])

        df = (students
              .merge(att_term, on=["student_id"])
              .merge(lms_term, on=["student_id","term"])
              .merge(assessments, on=["student_id","term"]))
        df["attendance_rate"] = df["attended"] / df["total_sessions"]
        df["activity_decile"] = pd.qcut(df["clicks"].rank(method="first"), 10, labels=False) + 1
        df["on_time_rate"] = 1 - (df["late_submissions"] / 5)
        df["at_risk"] = ((df["attendance_rate"] < 0.80) | (df["activity_decile"] <= 2)) & (df["midterm"] < 50)

        write_sqlite(con, "analytic_student_term", df,
                     ["student_id","term","program","intake"])

        # Sanity prints
        print(f"[INFO] Database created: {args.db}")
        for t in ["students","attendance","lms_activity","assessments","student_events",
                  "attendance_term","lms_term","analytic_student_term"]:
            cur = con.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:22s}: {cur.fetchone()[0]:,} rows")
        return 0
    except Exception as e:
        print("[ERROR] Loader failed:", repr(e))
        return 1
    finally:
        con.close()
        print("[INFO] Connection closed.")

if __name__ == "__main__":
    sys.exit(main())