import os

import pandas as pd
import plotly.express as px
import streamlit as st

from analysis import (
    baseline_counts,
    baseline_melanoma_pbmc_miraclib,
    build_summary_table,
    responder_comparison_table,
    responder_stats,
)
from db_utils import connect, load_csv_to_db


DATASET_PATH = os.path.join(os.path.dirname(__file__), "cell-count.csv")
DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "cell_counts.sqlite")


st.set_page_config(page_title="Immune Cell Dashboard", layout="wide")


st.title("Immune Cell Population Analysis")
st.markdown(
    "This dashboard loads `cell-count.csv` into SQLite, "
    "computes per-sample population frequencies, and compares "
    "melanoma miraclib PBMC responders vs non-responders."
)


with st.sidebar:
    st.header("Database")
    db_path = st.text_input("SQLite DB path", value=DEFAULT_DB_PATH)
    load_db = st.button("Initialize / Reload DB")
    st.caption("The database is rebuilt from the CSV when reloaded.")


if load_db or not os.path.exists(db_path):
    load_csv_to_db(DATASET_PATH, db_path)
    st.success("Database initialized.")


@st.cache_data(show_spinner=False)
def fetch_summary(db_path: str) -> pd.DataFrame:
    conn = connect(db_path)
    try:
        return build_summary_table(conn)
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def fetch_comparison(db_path: str) -> pd.DataFrame:
    conn = connect(db_path)
    try:
        return responder_comparison_table(conn)
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def fetch_stats(db_path: str) -> pd.DataFrame:
    conn = connect(db_path)
    try:
        comparison = responder_comparison_table(conn)
        return responder_stats(comparison)
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def fetch_baseline(db_path: str):
    conn = connect(db_path)
    try:
        baseline = baseline_melanoma_pbmc_miraclib(conn)
        return baseline, baseline_counts(baseline)
    finally:
        conn.close()


st.subheader("Part 2: Cell Population Frequencies")
summary_df = fetch_summary(db_path)
st.dataframe(summary_df, use_container_width=True, height=350)


st.subheader("Part 3: Responders vs Non-Responders")
comparison_df = fetch_comparison(db_path)
if comparison_df.empty:
    st.info("No melanoma miraclib PBMC samples found.")
else:
    fig = px.box(
        comparison_df,
        x="population",
        y="percentage",
        color="response",
        points="all",
        title="Relative frequency by response",
        labels={"percentage": "Relative frequency (%)", "population": "Population"},
    )
    st.plotly_chart(fig, use_container_width=True)

    stats_df = fetch_stats(db_path)
    st.markdown("**Significance testing (Mann-Whitney U with BH adjustment)**")
    st.dataframe(stats_df, use_container_width=True, height=250)


st.subheader("Part 4: Baseline Melanoma PBMC (Miraclib)")
baseline_df, baseline_breakdown = fetch_baseline(db_path)
st.dataframe(baseline_df, use_container_width=True, height=250)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**Samples per project**")
    st.dataframe(baseline_breakdown["samples_per_project"], use_container_width=True)
with col2:
    st.markdown("**Subjects by response**")
    st.dataframe(baseline_breakdown["subjects_by_response"], use_container_width=True)
with col3:
    st.markdown("**Subjects by sex**")
    st.dataframe(baseline_breakdown["subjects_by_sex"], use_container_width=True)
