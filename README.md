# Student Trajectory App 📊

An end-to-end data analytics project designed to track and analyze **student engagement and performance** over time.  
Built with **Python, SQLite, Pandas, and Tableau** for data processing, cleaning, and visualization.

---

## 🔍 Features
- **Synthetic Data Generation**: Simulated student records (attendance, assessments, LMS activity, advisor notes).
- **ETL Pipeline**: Extract, transform, and load (ETL) workflows in Python with SQLite.
- **Data Quality Checks**: Scripts to ensure no duplicates or missing values.
- **Visualization**: Tableau dashboards for student performance and engagement trends.
- **Scalable Setup**: Works with both small datasets and large student populations.

---

## 🛠 Tech Stack
- **Language:** Python (3.9+)
- **Database:** SQLite (`engagement.db`)
- **Libraries:** Pandas, NumPy, SQLite3
- **Visualization:** Tableau

---

## 📂 Project Structure

student-trajectory-app/
├── data/                  # Database & sample data
│   ├── engagement.db      # SQLite database
│   ├── sample_data_preview.csv
│   └── data_quality_report.md
├── notebooks/             # (Optional) Jupyter notebooks
├── reports/figures/       # Figures for reports
├── src/                   # Source code
│   ├── app/               # Main application
│   │   └── trajectory_app.py
│   └── sql/               # SQL scripts
│       ├── export_to_csv.py
│       └── load_to_sqlite.py
├── check_db.py            # Script to check DB connectivity
├── check_data_quality.py  # Data quality checks
├── list_tables.py         # List all tables in the database
├── requirements.txt       # Python dependencies
└── README.md

---

## 🚀 Getting Started

1. **Clone this repository**
   ```bash
   git clone https://github.com/hellosultan/student-trajectory-app.git
   cd student-trajectory-app

2.	Install dependencies (Python 3.9+)
python3 -m pip install -r requirements.txt

3.	Check the database connection
python3 check_db.py

4.	Run a data quality check
python3 check_data_quality.py

5.	List all tables in the database
python3 list_tables.py

📊 Visualization (Tableau)
	1.	Open Tableau Desktop (or Tableau Public).
	2.	Connect to the SQLite database (data/engagement.db).
	3.	Build dashboards for:
	•	Attendance trends
	•	LMS activity
	•	Assessment performance
	•	Advisor notes (qualitative insights)

🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

