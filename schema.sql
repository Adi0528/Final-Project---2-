DROP TABLE IF EXISTS sample_features;
DROP TABLE IF EXISTS sample_labels;
DROP TABLE IF EXISTS samples;
DROP TABLE IF EXISTS features;
DROP TABLE IF EXISTS labels;

CREATE TABLE samples (
  sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE features (
  feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
  feature_name TEXT UNIQUE NOT NULL
);

CREATE TABLE sample_features (
  sample_id INTEGER NOT NULL,
  feature_id INTEGER NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (sample_id, feature_id),
  FOREIGN KEY (sample_id) REFERENCES samples(sample_id),
  FOREIGN KEY (feature_id) REFERENCES features(feature_id)
);

CREATE TABLE labels (
  label_id INTEGER PRIMARY KEY AUTOINCREMENT,
  label_name TEXT UNIQUE NOT NULL
);

CREATE TABLE sample_labels (
  sample_id INTEGER PRIMARY KEY,
  label_id INTEGER NOT NULL,
  FOREIGN KEY (sample_id) REFERENCES samples(sample_id),
  FOREIGN KEY (label_id) REFERENCES labels(label_id)
);
