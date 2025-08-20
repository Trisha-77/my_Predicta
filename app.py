from flask import Flask, render_template, request, Response, send_file, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import os, io, csv

app = Flask(__name__)

# --- Database Config ---
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///plfs.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# --- Database Model ---
class PLFSRecord(db.Model):
    __tablename__ = "PLFSRecord"
    id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.String(50))
    gender = db.Column(db.String(10))
    age = db.Column(db.Integer)
    year = db.Column(db.String(10))
    employment_status = db.Column(db.String(50))
    wage = db.Column(db.Integer)

# --- Load CSV into DB ---
DATA_FILE = os.path.join("data", "sample_plfs.csv")
TEST_MODE = True   # Turn off before submission for privacy suppression

def load_csv_to_db():
    print("📥 Reloading CSV into SQLite database...")
    df = pd.read_csv(DATA_FILE)

    records = [
        PLFSRecord(
            state=str(row["state"]).strip(),
            gender=str(row["gender"]).strip(),
            age=int(row["age"]),
            year=str(row["year"]).strip(),
            employment_status=str(row["employment_status"]).strip(),
            wage=int(row["wage"])
        )
        for _, row in df.iterrows()
    ]
    db.session.bulk_save_objects(records)
    db.session.commit()
    print(f"✅ Data loaded into database. Rows: {len(records)}")
    print(df.head())

with app.app_context():
    db.drop_all()   # 🔥 ensure clean reset
    db.create_all()
    load_csv_to_db()

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/explore", methods=["GET", "POST"])
def explore():
    filters = {}
    query = PLFSRecord.query
    results = []

    if request.method == "POST":
        filters = {
            "state": request.form.get("state"),
            "gender": request.form.get("gender"),
            "employment_status": request.form.get("employment_status"),
            "year": request.form.get("year"),
            "chart_type": request.form.get("chart_type") or "bar"
        }

        # Apply filters dynamically
        if filters["state"]:
            query = query.filter(PLFSRecord.state == filters["state"])
        if filters["gender"]:
            query = query.filter(PLFSRecord.gender == filters["gender"])
        if filters["employment_status"]:
            query = query.filter(PLFSRecord.employment_status == filters["employment_status"])
        if filters["year"]:
            query = query.filter(PLFSRecord.year == filters["year"])

        results = query.all()

        # privacy suppression
        if not TEST_MODE and len(results) < 5:
            results = []
    else:
        filters = {"chart_type": "bar"}

    # --- Dropdowns (unique + sorted + cleaned) ---
    states = sorted({row[0].strip() for row in db.session.query(PLFSRecord.state).distinct().all() if row[0]})
    genders = sorted({row[0].strip() for row in db.session.query(PLFSRecord.gender).distinct().all() if row[0]})
    emp_status = sorted({row[0].strip() for row in db.session.query(PLFSRecord.employment_status).distinct().all() if row[0]})
    years = sorted({row[0].strip() for row in db.session.query(PLFSRecord.year).distinct().all() if row[0]})

    print("States:", states)
    print("Genders:", genders)
    print("Employment:", emp_status)
    print("Years:", years)

    # Convert results to list of dicts for template
    results_dict = [r.__dict__ for r in results]
    for r in results_dict:
        r.pop("_sa_instance_state", None)

    return render_template("explore.html",
                           filters=filters,
                           results=results_dict,
                           states=states,
                           genders=genders,
                           emp_status=emp_status,
                           years=years)

# --- CSV Download ---
@app.route("/download_csv", methods=["POST"])
def download_csv():
    query = PLFSRecord.query
    filters = {
        "state": request.form.get("state"),
        "gender": request.form.get("gender"),
        "employment_status": request.form.get("employment_status"),
        "year": request.form.get("year"),
    }

    if filters["state"]:
        query = query.filter(PLFSRecord.state == filters["state"])
    if filters["gender"]:
        query = query.filter(PLFSRecord.gender == filters["gender"])
    if filters["employment_status"]:
        query = query.filter(PLFSRecord.employment_status == filters["employment_status"])
    if filters["year"]:
        query = query.filter(PLFSRecord.year == filters["year"])

    results = query.all()
    if not TEST_MODE and len(results) < 5:
        results = []

    si = io.StringIO()
    if results:
        writer = csv.DictWriter(si, fieldnames=["state","gender","age","year","employment_status","wage"])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "state": r.state,
                "gender": r.gender,
                "age": r.age,
                "year": r.year,
                "employment_status": r.employment_status,
                "wage": r.wage
            })

    output = si.getvalue()
    return Response(output, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=results.csv"})

# --- Excel Download ---
@app.route("/download_excel", methods=["POST"])
def download_excel():
    query = PLFSRecord.query
    filters = {
        "state": request.form.get("state"),
        "gender": request.form.get("gender"),
        "employment_status": request.form.get("employment_status"),
        "year": request.form.get("year"),
    }

    if filters["state"]:
        query = query.filter(PLFSRecord.state == filters["state"])
    if filters["gender"]:
        query = query.filter(PLFSRecord.gender == filters["gender"])
    if filters["employment_status"]:
        query = query.filter(PLFSRecord.employment_status == filters["employment_status"])
    if filters["year"]:
        query = query.filter(PLFSRecord.year == filters["year"])

    results = query.all()
    if not TEST_MODE and len(results) < 5:
        results = []

    df = pd.DataFrame([{
        "state": r.state,
        "gender": r.gender,
        "age": r.age,
        "year": r.year,
        "employment_status": r.employment_status,
        "wage": r.wage
    } for r in results])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="PLFS_Data")
    output.seek(0)

    return send_file(output, as_attachment=True,
                     download_name="results.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# --- API Endpoints ---
@app.route("/api/data", methods=["GET"])
def api_data():
    results = PLFSRecord.query.all()
    return jsonify([{
        "state": r.state,
        "gender": r.gender,
        "age": r.age,
        "year": r.year,
        "employment_status": r.employment_status,
        "wage": r.wage
    } for r in results])

@app.route("/api/filter", methods=["POST"])
def api_filter():
    query = PLFSRecord.query
    filters = request.json

    if filters.get("state"):
        query = query.filter(PLFSRecord.state == filters["state"])
    if filters.get("gender"):
        query = query.filter(PLFSRecord.gender == filters["gender"])
    if filters.get("employment_status"):
        query = query.filter(PLFSRecord.employment_status == filters["employment_status"])
    if filters.get("year"):
        query = query.filter(PLFSRecord.year == filters["year"])

    results = query.all()
    return jsonify([{
        "state": r.state,
        "gender": r.gender,
        "age": r.age,
        "year": r.year,
        "employment_status": r.employment_status,
        "wage": r.wage
    } for r in results])

if __name__ == "__main__":
    app.run(debug=True)
