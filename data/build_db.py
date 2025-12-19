from pathlib import Path
import sqlite3
import pandas as pd

RAW_CSV = Path("data/raw/dataset.csv")
DB_PATH = Path("data/db/dataset.db")

def main():
    df = pd.read_csv(RAW_CSV)

    # Create IDs (stable enough for this project)
    df = df.reset_index(drop=True)
    df["person_id"] = df.index + 1

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # Drop if rerunning
        cur.executescript("""
        DROP TABLE IF EXISTS outcome;
        DROP TABLE IF EXISTS labs;
        DROP TABLE IF EXISTS conditions;
        DROP TABLE IF EXISTS person;
        """)

        # 3NF-ish design: separate entities
        cur.executescript("""
        CREATE TABLE person (
            person_id INTEGER PRIMARY KEY,
            gender TEXT NOT NULL,
            age REAL NOT NULL
        );

        CREATE TABLE conditions (
            person_id INTEGER PRIMARY KEY,
            hypertension INTEGER NOT NULL CHECK (hypertension IN (0,1)),
            heart_disease INTEGER NOT NULL CHECK (heart_disease IN (0,1)),
            smoking_history TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES person(person_id)
        );

        CREATE TABLE labs (
            person_id INTEGER PRIMARY KEY,
            bmi REAL NOT NULL,
            hba1c_level REAL NOT NULL,
            blood_glucose_level REAL NOT NULL,
            FOREIGN KEY (person_id) REFERENCES person(person_id)
        );

        CREATE TABLE outcome (
            person_id INTEGER PRIMARY KEY,
            diabetes INTEGER NOT NULL CHECK (diabetes IN (0,1)),
            FOREIGN KEY (person_id) REFERENCES person(person_id)
        );
        """)

        # Insert rows
        person = df[["person_id", "gender", "age"]]
        conditions = df[["person_id", "hypertension", "heart_disease", "smoking_history"]]
        labs = df[["person_id", "bmi", "HbA1c_level", "blood_glucose_level"]].rename(
            columns={"HbA1c_level": "hba1c_level"}
        )
        outcome = df[["person_id", "diabetes"]]

        person.to_sql("person", conn, if_exists="append", index=False)
        conditions.to_sql("conditions", conn, if_exists="append", index=False)
        labs.to_sql("labs", conn, if_exists="append", index=False)
        outcome.to_sql("outcome", conn, if_exists="append", index=False)

    print(f"✅ Built DB at: {DB_PATH}")

if __name__ == "__main__":
    main()
