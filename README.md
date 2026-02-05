# Immune Cell Population Analysis

This project loads `cell-count.csv` into a SQLite database, computes per-sample
cell population frequencies, and visualizes responder vs non-responder patterns
for melanoma patients treated with miraclib.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the dashboard

```bash
streamlit run app.py
```

The app will create (or refresh) `cell_counts.sqlite` automatically from the CSV.

## Database schema and rationale

The database is normalized into three tables:

- `subjects(subject_id, condition, age, sex)`
- `samples(sample_id, project, subject_id, treatment, response, sample_type, time_from_treatment_start)`
- `counts(sample_id, population, count)`

Rationale:

- `subjects` stores patient-level attributes that do not change per sample.
- `samples` stores the sampling event and trial metadata.
- `counts` stores one row per immune population per sample.

This design reduces duplication (e.g., subject metadata repeated across samples)
and makes it easy to add new populations or new metrics without altering the
core entities. It scales to hundreds of projects and thousands of samples by
adding indexes (e.g., on `samples.project`, `samples.treatment`, `samples.response`,
`samples.sample_type`, and `counts.population`) and by partitioning or sharding
if the counts table becomes very large. Additional analytics can be added as
derived tables or views without changing the raw data model.

## Code structure

- `db_utils.py`: schema definition and CSV loading into SQLite.
- `analysis.py`: SQL queries and analysis utilities (summary table, responder
  comparison, baseline subsets, and statistics).
- `app.py`: Streamlit dashboard with interactive tables and boxplots.

This separation keeps data ingestion, analysis, and presentation concerns
independent and easy to test or extend.

## Dashboard link

If deployed, add the URL here (e.g., Streamlit Community Cloud).
For local use, run `streamlit run app.py` and open the local URL shown in the console.
