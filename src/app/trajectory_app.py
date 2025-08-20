#!/usr/bin/env python
"""
Student Trajectory App — Interactive (clean build)
- Sidebar: program/intake, term multiselect (with fallback), risk sliders, date window, search
- Cohort snapshot expander with metrics + CSV
- Student profile: KPIs, Timeline, Attendance, LMS, Assessments, Data & Notes
- Deep link: ?sid=<student_id>
"""
import os, sqlite3, subprocess
from datetime import datetime
import pandas as pd
import streamlit as st
import altair as alt

DB_PATH = os.path.join("data", "engagement.db")

# ===================== Data helpers =====================
@st.cache_data(show_spinner=False)
def load_sql(query: str, params: tuple = ()):
    con = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, con, params=params)
    finally:
        con.close()

@st.cache_data(show_spinner=False)
def list_students(q=None, program=None, intake=None):
    where, params = [], []
    if q:
        where.append("(full_name LIKE ? OR CAST(student_id AS TEXT)=?)")
        params.extend([f"%{q}%", q])
    if program and program != "All":
        where.append("program = ?"); params.append(program)
    if intake and intake != "All":
        where.append("intake = ?"); params.append(intake)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    return load_sql(
        f"SELECT student_id, full_name, program, intake, enrol_date "
        f"FROM students {clause} ORDER BY full_name ASC;",
        tuple(params)
    )

@st.cache_data(show_spinner=False)
def student_profile(student_id: int):
    s = load_sql("SELECT * FROM students WHERE student_id = ?;", (student_id,))
    ast = load_sql("SELECT * FROM analytic_student_term WHERE student_id = ? ORDER BY term;", (student_id,))
    att = load_sql("SELECT week, term, sessions, attended FROM attendance WHERE student_id = ? ORDER BY term, week;", (student_id,))
    lms = load_sql("SELECT activity_date, clicks FROM lms_activity WHERE student_id = ? ORDER BY activity_date;", (student_id,))
    ev  = load_sql("SELECT event_type, event_date, term, details FROM student_events WHERE student_id = ? ORDER BY event_date;", (student_id,))
    if not lms.empty: lms["activity_date"] = pd.to_datetime(lms["activity_date"])
    if not ev.empty:  ev["event_date"]   = pd.to_datetime(ev["event_date"])
    return (s.iloc[0] if not s.empty else None), ast, att, lms, ev

# ----------------- Notes helpers -----------------
def init_notes_table():
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS advisor_notes (
                student_id INTEGER,
                note TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.commit()
    finally:
        con.close()

def add_note(student_id:int, note:str):
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("INSERT INTO advisor_notes (student_id, note) VALUES (?, ?);", (student_id, note))
        con.commit()
    finally:
        con.close()

def get_notes(student_id:int):
    return load_sql(
        "SELECT note, created_at FROM advisor_notes WHERE student_id = ? ORDER BY created_at DESC;",
        (student_id,)
    )

# ===================== App =====================
def main():
    st.set_page_config(page_title="Student Trajectory", page_icon="🎓", layout="wide")
    st.title("🎓 Student Trajectory Explorer")
    st.caption("Synthetic demo DB (deterministic RNG=42). Use the sidebar to filter and pick a student.")

    # Ensure DB exists (auto-build if missing)
    if not os.path.exists(DB_PATH):
        os.makedirs("data", exist_ok=True)
        subprocess.run(["python", "src/sql/load_to_sqlite.py"], check=True)

    init_notes_table()

    # Deep link: read ?sid= once
    qp = st.query_params
    if "sid" in qp and qp.get("sid"):
        try:
            st.session_state.setdefault("sid_prefill", int(qp.get("sid")))
        except Exception:
            pass

    # ---------- Sidebar ----------
    st.sidebar.header("Filters")

    df_programs = load_sql("SELECT DISTINCT program FROM students ORDER BY program;")
    programs = ["All"] + df_programs["program"].tolist()
    program = st.sidebar.selectbox("Program", programs, index=0)

    df_intakes = load_sql("SELECT DISTINCT intake FROM students ORDER BY intake;")
    intakes = ["All"] + df_intakes["intake"].tolist()
    intake = st.sidebar.selectbox("Intake", intakes, index=0)

    # Terms with default + fallback
    terms_all = load_sql("SELECT DISTINCT term FROM analytic_student_term ORDER BY term;")["term"].tolist()
    terms_sel = st.sidebar.multiselect("Show terms", terms_all, default=terms_all)
    if not terms_sel:
        terms_sel = terms_all

    # Search + choose student
    query = st.sidebar.text_input("Search name or ID")
    matches = list_students(query if query else None, program, intake)
    st.sidebar.caption(f"Matches: **{len(matches)}**")
    sid = None
    if not matches.empty:
        matches["label"] = matches["student_id"].astype(str) + " — " + matches["full_name"]
        chosen = st.sidebar.selectbox("Choose student", matches["label"].tolist())
        sid = int(chosen.split(" — ")[0])

    # Risk rule sliders
    st.sidebar.header("Risk rule")
    th_att = st.sidebar.slider("Min attendance %", 0, 100, 80, 1)
    th_act = st.sidebar.slider("Min activity decile", 1, 10, 3, 1)
    th_mid = st.sidebar.slider("Midterm threshold", 0, 100, 50, 1)
    st.sidebar.caption("At-risk if (attendance < A) OR (activity ≤ B) AND (midterm < C)")

    # Date window (plain Python datetimes)
    min_date = datetime(2024, 9, 1)
    max_date = datetime(2025, 6, 30)
    date_range = st.sidebar.slider("Limit charts to date range",
                                   min_value=min_date, max_value=max_date,
                                   value=(min_date, max_date))

    # ---------- Cohort snapshot ----------
    with st.expander("Cohort snapshot (current filters)"):
        cohort = load_sql("SELECT * FROM analytic_student_term;")

        # live risk recompute
        cohort["at_risk_live"] = (
            ((cohort["attendance_rate"] < (th_att / 100)) | (cohort["activity_decile"] <= th_act))
            & (cohort["midterm"] < th_mid)
        )

        # apply filters
        if program != "All":
            cohort = cohort[cohort["program"] == program]
        if intake != "All":
            cohort = cohort[cohort["intake"] == intake]
        cohort = cohort[cohort["term"].isin(terms_sel)]

        if cohort.empty:
            st.info("No data with current filters. Try selecting more terms in the sidebar.")
        else:
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1: st.metric("Students", f"{cohort['student_id'].nunique():,}")
            with k2: st.metric("Avg Attendance", f"{cohort['attendance_rate'].mean():.1%}")
            with k3: st.metric("Avg Activity Decile", f"{cohort['activity_decile'].mean():.1f}")
            with k4: st.metric("Avg Midterm", f"{cohort['midterm'].mean():.1f}")
            with k5: st.metric("At-Risk % (live)", f"{cohort['at_risk_live'].mean():.1%}")

            st.dataframe(
                cohort[["student_id","term","program","intake","attendance_rate",
                        "activity_decile","midterm","final","at_risk_live"]]
                .sort_values(["student_id","term"]),
                use_container_width=True
            )
            st.download_button(
                "Download cohort CSV",
                cohort.to_csv(index=False).encode("utf-8"),
                file_name="cohort_snapshot.csv", mime="text/csv"
            )

    # Pre-fill sid from URL, once
    if "sid_prefill" in st.session_state and st.session_state["sid_prefill"] and sid is None:
        sid = int(st.session_state.pop("sid_prefill"))
    if sid is not None:
        st.query_params.update({"sid": str(sid)})

    if sid is None:
        st.info("Pick a student from the sidebar to see their full trajectory.")
        st.stop()

    # ---------- Student profile ----------
    student, ast, att, lms, ev = student_profile(sid)
    if student is None:
        st.warning("Student not found."); st.stop()

    # Filter by selected terms
    ast = ast[ast["term"].isin(terms_sel)]
    att = att[att["term"].isin(terms_sel)]

    if ast.empty or att.empty:
        st.info("No data with current filters. Try selecting more terms in the sidebar.")
        st.stop()

    st.subheader(f"{student['full_name']}  ·  ID {int(student['student_id'])}")
    st.caption(f"{student['program']} · Intake {student['intake']} · Enrolled {student['enrol_date']}")

    # Live risk flag on the student-term rows
    ast["at_risk_live"] = (
        ((ast["attendance_rate"] < (th_att/100)) | (ast["activity_decile"] <= th_act))
        & (ast["midterm"] < th_mid)
    )

    # KPI cards (latest term)
    latest = ast.sort_values("term").tail(1)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Attendance", f"{latest['attendance_rate'].iloc[0]:.1%}")
    with c2: st.metric("Activity Decile", f"{latest['activity_decile'].iloc[0]:.0f}/10")
    with c3: st.metric("On-time Submissions", f"{latest['on_time_rate'].iloc[0]:.1%}")
    with c4: st.metric("Midterm", f"{latest['midterm'].iloc[0]:.1f}")
    with c5: st.metric("At-Risk (live)", "Yes" if latest['at_risk_live'].iloc[0] else "No")

    # Tabs
    tab_tl, tab_att, tab_lms, tab_asmt, tab_data = st.tabs(
        ["🕘 Timeline", "📅 Attendance", "🖱️ LMS Activity", "📝 Assessments", "📦 Data & Notes"]
    )

    with tab_tl:
        st.caption("Lifecycle events for this student (use sidebar date window).")
        if not ev.empty:
            ev_f = ev[(ev["event_date"] >= date_range[0]) & (ev["event_date"] <= date_range[1])]
            timeline = alt.Chart(ev_f).mark_circle(size=120).encode(
                x=alt.X('event_date:T', title="Date"),
                y=alt.value(0),
                color=alt.Color('event_type:N', legend=alt.Legend(title="Event")),
                tooltip=['event_type','event_date','term','details']
            ).properties(height=120)
            st.altair_chart(timeline, use_container_width=True)
        else:
            st.info("No events recorded.")

    with tab_att:
        st.caption("Weekly attendance by term; hover for details.")
        att_plot = att.copy()
        att_plot["pct"] = att_plot["attended"] / att_plot["sessions"]
        chart_att = alt.Chart(att_plot).mark_line(point=True).encode(
            x=alt.X('week:O', title='Teaching Week'),
            y=alt.Y('pct:Q', title='Attendance %', scale=alt.Scale(domain=[0,1])),
            color='term:N',
            tooltip=['term','week','attended','sessions','pct']
        ).properties(height=280)
        st.altair_chart(chart_att, use_container_width=True)
        st.download_button(
            "Download attendance (this student)",
            att_plot.to_csv(index=False).encode("utf-8"),
            file_name=f"student_{int(student['student_id'])}_attendance.csv", mime="text/csv"
        )

    with tab_lms:
        st.caption("Daily clicks & 7‑day rolling (filtered by date window).")
        if not lms.empty:
            l = lms.copy().sort_values("activity_date")
            l = l[l["activity_date"].between(date_range[0], date_range[1])]
            l["rolling_7d"] = l["clicks"].rolling(7, min_periods=1).sum()
            long = l.melt(id_vars=["activity_date"], value_vars=["clicks","rolling_7d"],
                          var_name="metric", value_name="value")
            chart_lms = alt.Chart(long).mark_line().encode(
                x=alt.X('activity_date:T', title='Date'),
                y=alt.Y('value:Q', title='Clicks'),
                color='metric:N',
                tooltip=['activity_date:T','metric:N','value:Q']
            ).properties(height=280)
            st.altair_chart(chart_lms, use_container_width=True)
            st.download_button(
                "Download LMS (this student)",
                l.to_csv(index=False).encode("utf-8"),
                file_name=f"student_{int(student['student_id'])}_lms.csv", mime="text/csv"
            )
        else:
            st.info("No LMS rows in window.")

    with tab_asmt:
        st.caption("Scores per term with live at‑risk flag.")
        long_scores = ast.melt(id_vars=["term"], value_vars=["midterm","final"],
                               var_name="assessment", value_name="score")
        bar = alt.Chart(long_scores).mark_bar().encode(
            x=alt.X('term:N', title="Term"),
            y=alt.Y('score:Q', title="Score"),
            color='assessment:N',
            tooltip=['term','assessment','score']
        ).properties(height=280)
        st.altair_chart(bar, use_container_width=True)
        st.dataframe(
            ast.sort_values("term")[["term","attendance_rate","activity_decile","midterm","final","at_risk_live"]],
            use_container_width=True
        )

    with tab_data:
        st.caption("All joined data for this student (filtered).")
        st.download_button(
            "Download student-term CSV",
            ast.sort_values("term").to_csv(index=False).encode("utf-8"),
            file_name=f"student_{int(student['student_id'])}_terms.csv", mime="text/csv"
        )
        st.markdown("---")
        st.subheader("Advisor Notes")
        new_note = st.text_area("Add note", placeholder="e.g., Met on 2025‑02‑10; agreed to weekly study plan.")
        colA, colB = st.columns([1,3])
        with colA:
            if st.button("Save note", use_container_width=True, disabled=(not new_note.strip())):
                add_note(int(student["student_id"]), new_note.strip())
                st.success("Note saved."); st.experimental_rerun()
        notes = get_notes(int(student["student_id"]))
        st.dataframe(notes, use_container_width=True)

if __name__ == "__main__":
    main()