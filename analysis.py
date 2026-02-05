from __future__ import annotations

import sqlite3
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from db_utils import POPULATIONS, connect


def build_summary_table(conn: sqlite3.Connection) -> pd.DataFrame:
    query = """
    WITH totals AS (
        SELECT sample_id, SUM(count) AS total_count
        FROM counts
        GROUP BY sample_id
    )
    SELECT
        s.sample_id AS sample,
        t.total_count,
        c.population,
        c.count,
        ROUND((c.count * 100.0) / t.total_count, 3) AS percentage
    FROM counts c
    JOIN totals t ON t.sample_id = c.sample_id
    JOIN samples s ON s.sample_id = c.sample_id
    ORDER BY s.sample_id, c.population
    """
    return pd.read_sql_query(query, conn)


def responder_comparison_table(conn: sqlite3.Connection) -> pd.DataFrame:
    query = """
    WITH totals AS (
        SELECT sample_id, SUM(count) AS total_count
        FROM counts
        GROUP BY sample_id
    )
    SELECT
        s.sample_id AS sample,
        s.response,
        c.population,
        c.count,
        (c.count * 100.0) / t.total_count AS percentage
    FROM counts c
    JOIN totals t ON t.sample_id = c.sample_id
    JOIN samples s ON s.sample_id = c.sample_id
    JOIN subjects subj ON subj.subject_id = s.subject_id
    WHERE
        subj.condition = 'melanoma'
        AND s.treatment = 'miraclib'
        AND s.sample_type = 'PBMC'
        AND s.response IN ('yes', 'no')
    """
    return pd.read_sql_query(query, conn)


def _benjamini_hochberg(p_values: np.ndarray) -> np.ndarray:
    if p_values.size == 0:
        return p_values
    order = np.argsort(p_values)
    ranked = p_values[order]
    n = p_values.size
    adjusted = np.empty_like(ranked)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        value = (ranked[i] * n) / rank
        prev = min(prev, value)
        adjusted[i] = prev
    adjusted = np.clip(adjusted, 0.0, 1.0)
    result = np.empty_like(adjusted)
    result[order] = adjusted
    return result


def responder_stats(df: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    results = []
    for population in POPULATIONS:
        subset = df[df["population"] == population]
        responders = subset[subset["response"] == "yes"]["percentage"]
        non_responders = subset[subset["response"] == "no"]["percentage"]

        if responders.empty or non_responders.empty:
            p_value = np.nan
        else:
            stat = mannwhitneyu(responders, non_responders, alternative="two-sided")
            p_value = stat.pvalue

        results.append(
            {
                "population": population,
                "n_yes": int(responders.shape[0]),
                "n_no": int(non_responders.shape[0]),
                "p_value": p_value,
            }
        )

    stats_df = pd.DataFrame(results)
    stats_df["p_value_adj"] = _benjamini_hochberg(stats_df["p_value"].to_numpy())
    stats_df["significant"] = stats_df["p_value_adj"] < alpha
    return stats_df


def baseline_melanoma_pbmc_miraclib(conn: sqlite3.Connection) -> pd.DataFrame:
    query = """
    SELECT
        s.sample_id,
        s.project,
        s.subject_id,
        s.response,
        subj.sex,
        s.time_from_treatment_start
    FROM samples s
    JOIN subjects subj ON subj.subject_id = s.subject_id
    WHERE
        subj.condition = 'melanoma'
        AND s.sample_type = 'PBMC'
        AND s.treatment = 'miraclib'
        AND s.time_from_treatment_start = 0
    """
    return pd.read_sql_query(query, conn)


def baseline_counts(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    samples_per_project = (
        df.groupby("project")["sample_id"]
        .nunique()
        .reset_index(name="sample_count")
    )
    subjects_by_response = (
        df.drop_duplicates(subset=["subject_id"])
        .groupby("response")["subject_id"]
        .nunique()
        .reset_index(name="subject_count")
    )
    subjects_by_sex = (
        df.drop_duplicates(subset=["subject_id"])
        .groupby("sex")["subject_id"]
        .nunique()
        .reset_index(name="subject_count")
    )
    return {
        "samples_per_project": samples_per_project,
        "subjects_by_response": subjects_by_response,
        "subjects_by_sex": subjects_by_sex,
    }


def load_all(db_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
    conn = connect(db_path)
    try:
        summary = build_summary_table(conn)
        comparison = responder_comparison_table(conn)
        stats = responder_stats(comparison)
        baseline = baseline_melanoma_pbmc_miraclib(conn)
        baseline_breakdown = baseline_counts(baseline)
        return summary, comparison, stats, baseline_breakdown
    finally:
        conn.close()
