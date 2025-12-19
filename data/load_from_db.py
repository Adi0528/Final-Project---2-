from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("data/db/dataset.db")

def load_dataframe() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        query = """
        SELECT
            p.person_id,
            p.gender,
            p.age,
            c.hypertension,
            c.heart_disease,
            c.smoking_history,
            l.bmi,
            l.hba1c_level,
            l.blood_glucose_level,
            o.diabetes
        FROM person p
        JOIN conditions c USING (person_id)
        JOIN labs l USING (person_id)
        JOIN outcome o USING (person_id);
        """
        df = pd.read_sql_query(query, conn)
    return df

def load_xy():
    df = load_dataframe()
    X = df.drop(columns=["person_id", "diabetes"])
    y = df["diabetes"].astype(int)
    return X, y
