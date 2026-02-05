import csv
import os
import sqlite3
from typing import Iterable


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS subjects (
    subject_id TEXT PRIMARY KEY,
    condition TEXT NOT NULL,
    age INTEGER NOT NULL,
    sex TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    project TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    treatment TEXT NOT NULL,
    response TEXT NOT NULL,
    sample_type TEXT NOT NULL,
    time_from_treatment_start INTEGER NOT NULL,
    FOREIGN KEY(subject_id) REFERENCES subjects(subject_id)
);

CREATE TABLE IF NOT EXISTS counts (
    sample_id TEXT NOT NULL,
    population TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (sample_id, population),
    FOREIGN KEY(sample_id) REFERENCES samples(sample_id)
);
"""


POPULATIONS = ["b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def _insert_many(conn: sqlite3.Connection, query: str, rows: Iterable[tuple]) -> None:
    conn.executemany(query, list(rows))
    conn.commit()


def load_csv_to_db(csv_path: str, db_path: str) -> None:
    conn = connect(db_path)
    try:
        initialize_schema(conn)

        subjects_rows = {}
        samples_rows = []
        counts_rows = []

        with open(csv_path, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                subject_id = row["subject"]
                if subject_id not in subjects_rows:
                    subjects_rows[subject_id] = (
                        subject_id,
                        row["condition"],
                        int(row["age"]),
                        row["sex"],
                    )

                sample_id = row["sample"]
                samples_rows.append(
                    (
                        sample_id,
                        row["project"],
                        subject_id,
                        row["treatment"],
                        row["response"],
                        row["sample_type"],
                        int(row["time_from_treatment_start"]),
                    )
                )

                for population in POPULATIONS:
                    counts_rows.append(
                        (
                            sample_id,
                            population,
                            int(row[population]),
                        )
                    )

        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO subjects
                (subject_id, condition, age, sex)
            VALUES (?, ?, ?, ?)
            """,
            subjects_rows.values(),
        )
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO samples
                (sample_id, project, subject_id, treatment, response, sample_type,
                 time_from_treatment_start)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            samples_rows,
        )
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO counts
                (sample_id, population, count)
            VALUES (?, ?, ?)
            """,
            counts_rows,
        )
    finally:
        conn.close()
