import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("db/classification.db")
SCHEMA_PATH = Path("db/schema.sql")

# ✅ CHANGE THESE TWO LINES FOR YOUR NEW DATASET
CSV_PATH = Path("data/raw/dataset.csv")
TARGET_COL = "target"   # <-- put your target column name here

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    if TARGET_COL not in df.columns:
        raise ValueError(f"Target '{TARGET_COL}' not found. Columns: {df.columns.tolist()}")

    y = df[TARGET_COL].copy()
    X = df.drop(columns=[TARGET_COL]).copy()

    # store everything as TEXT in DB (works for numeric + categorical)
    X = X.astype(str).fillna("")
    y = y.astype(str).fillna("")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript(SCHEMA_PATH.read_text())
    conn.commit()

    # labels
    for label in sorted(y.unique()):
        cur.execute("INSERT INTO labels(label_name) VALUES (?)", (label,))
    conn.commit()

    # features
    for col in X.columns:
        cur.execute("INSERT INTO features(feature_name) VALUES (?)", (col,))
    conn.commit()

    feature_map = dict(cur.execute("SELECT feature_name, feature_id FROM features").fetchall())
    label_map = dict(cur.execute("SELECT label_name, label_id FROM labels").fetchall())

    for i in range(len(X)):
        cur.execute("INSERT INTO samples(source) VALUES (?)", ("dataset_csv",))
        sample_id = cur.lastrowid

        label_id = label_map[str(y.iloc[i])]
        cur.execute("INSERT INTO sample_labels(sample_id, label_id) VALUES (?, ?)", (sample_id, label_id))

        row = X.iloc[i]
        rows = [(sample_id, feature_map[col], row[col]) for col in X.columns]
        cur.executemany(
            "INSERT INTO sample_features(sample_id, feature_id, value) VALUES (?, ?, ?)",
            rows
        )

        if i % 500 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    print(f"✅ Built DB: {DB_PATH} rows={len(X)} features={X.shape[1]}")
    print("Feature columns:", list(X.columns))

if __name__ == "__main__":
    main()
