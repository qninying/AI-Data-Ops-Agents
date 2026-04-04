"""
Daily production ETL — Oklahoma oilfield domain.
Usage:
    python3 -m etl.pipeline
    python3 -m etl.pipeline --failure-mode schema_drift
    python3 -m etl.pipeline --failure-mode null_explosion
    python3 -m etl.pipeline --failure-mode row_count_drop
    python3 -m etl.pipeline --failure-mode type_mismatch
"""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from database.db import engine, get_db
from config.settings import settings

EXPECTED_SCHEMA = [
    "api_number", "report_date",
    "oil_bbls", "gas_mcf", "water_bbls",
    "hours_on_production", "downtime_code",
]
EXPECTED_ROW_COUNT   = 10_000
NULL_RATE_THRESHOLD  = 0.05
MIN_COVERAGE_PCT     = 0.85


class FailureMode(str, Enum):
    NONE           = "none"
    SCHEMA_DRIFT   = "schema_drift"
    NULL_EXPLOSION  = "null_explosion"
    ROW_COUNT_DROP  = "row_count_drop"
    TYPE_MISMATCH   = "type_mismatch"


def get_active_api_numbers() -> list:
    with get_db() as db:
        rows = db.execute(text(
            "SELECT api_number FROM dbo.wells WHERE well_type='PRODUCER' AND status='ACTIVE'"
        )).fetchall()
    return [r[0] for r in rows]


def generate_production_report(report_date: Optional[date] = None) -> pd.DataFrame:
    if report_date is None:
        report_date = date.today() - timedelta(days=1)
    api_numbers = get_active_api_numbers()
    rng = np.random.default_rng(seed=int(report_date.strftime("%Y%m%d")))
    n = len(api_numbers)
    hours = np.where(rng.random(n) < 0.08, rng.uniform(4, 20, n), 24.0).round(2)
    downtime_pool = ["EQP", "EQP", "WX", "MAINT", "FLOW"]
    downtime_codes = [rng.choice(downtime_pool) if h < 24 else None for h in hours]
    uptime_factor = hours / 24.0
    base_oil  = rng.uniform(50, 700, n)
    oil_bbls  = (base_oil * uptime_factor + rng.normal(0, 15, n)).clip(0).round(2)
    gas_mcf   = (oil_bbls * rng.uniform(1.0, 2.5, n) + rng.normal(0, 20, n)).clip(0).round(3)
    water_bbls = (oil_bbls * rng.uniform(1.5, 6.0, n) + rng.normal(0, 30, n)).clip(0).round(2)
    return pd.DataFrame({
        "api_number":          api_numbers,
        "report_date":         report_date,
        "oil_bbls":            oil_bbls,
        "gas_mcf":             gas_mcf,
        "water_bbls":          water_bbls,
        "hours_on_production": hours,
        "downtime_code":       downtime_codes,
    })


# --- Failure injectors ---

def inject_schema_drift(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rng = np.random.default_rng(seed=1)
    df["condensate_bbls"] = (df["oil_bbls"] * rng.uniform(0.05, 0.15, len(df))).round(2)
    df["ngl_bbls"]        = (df["gas_mcf"]  * rng.uniform(0.02, 0.08, len(df))).round(2)
    df["api14_formatted"] = df["api_number"].str.replace("-", "")
    return df

def inject_null_explosion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mask = np.random.default_rng(seed=2).random(len(df)) < 0.38
    df.loc[mask, "oil_bbls"] = None
    df.loc[mask, "gas_mcf"]  = None
    return df

def inject_row_count_drop(df: pd.DataFrame) -> pd.DataFrame:
    return df.sample(n=min(4, len(df)), random_state=3)

def inject_type_mismatch(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["gas_mcf"] = df["gas_mcf"].apply(lambda v: f"{v:.3f} MCF" if pd.notna(v) else "N/A")
    return df

INJECTORS = {
    FailureMode.SCHEMA_DRIFT:   inject_schema_drift,
    FailureMode.NULL_EXPLOSION:  inject_null_explosion,
    FailureMode.ROW_COUNT_DROP:  inject_row_count_drop,
    FailureMode.TYPE_MISMATCH:   inject_type_mismatch,
}


# --- Validation ---

class ValidationResult:
    def __init__(self):
        self.passed = True
        self.errors: list = []
        self.stats:  dict = {}
    def fail(self, msg: str):
        self.passed = False
        self.errors.append(msg)


def validate(df: pd.DataFrame, wells_expected: int) -> ValidationResult:
    result = ValidationResult()
    extra   = set(df.columns) - set(EXPECTED_SCHEMA)
    missing = set(EXPECTED_SCHEMA) - set(df.columns)
    if extra:
        result.fail(f"Schema drift — unexpected columns: {sorted(extra)}")
    if missing:
        result.fail(f"Missing columns: {sorted(missing)}")
    result.stats["rows_received"]  = len(df)
    result.stats["wells_expected"] = wells_expected
    coverage = len(df) / wells_expected if wells_expected > 0 else 0
    result.stats["coverage_pct"] = round(coverage * 100, 2)
    if coverage < MIN_COVERAGE_PCT:
        result.fail(f"Row count drop — {len(df)} of {wells_expected} wells ({coverage:.1%})")
    if "oil_bbls" in df.columns:
        null_rate = df["oil_bbls"].isna().mean()
        result.stats["null_rate_pct"] = round(null_rate * 100, 2)
        if null_rate > NULL_RATE_THRESHOLD:
            result.fail(f"Null explosion — oil_bbls null rate: {null_rate:.1%}")
    if "gas_mcf" in df.columns:
        try:
            pd.to_numeric(df["gas_mcf"], errors="raise")
        except (ValueError, TypeError):
            result.fail("Type mismatch — gas_mcf is not numeric")
    return result


def schema_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(",".join(sorted(df.columns)).encode()).hexdigest()


def open_run(pipeline_name: str, source_file: str) -> int:
    with get_db() as db:
        result = db.execute(text("""
            INSERT INTO dbo.etl_run_log (pipeline_name, source_file, status)
            OUTPUT INSERTED.run_id
            VALUES (:pipeline_name, :source_file, 'RUNNING')
        """), {"pipeline_name": pipeline_name, "source_file": source_file})
        return result.fetchone()[0]


def close_run(run_id: int, status: str, stats: dict, failure_mode: str, error: Optional[str] = None):
    with get_db() as db:
        db.execute(text("""
            UPDATE dbo.etl_run_log SET
                run_end         = SYSUTCDATETIME(),
                status          = :status,
                rows_received   = :rows_received,
                rows_loaded     = :rows_loaded,
                rows_rejected   = :rows_rejected,
                wells_expected  = :wells_expected,
                coverage_pct    = :coverage_pct,
                null_rate_pct   = :null_rate_pct,
                failure_mode    = :failure_mode,
                error_message   = :error_message,
                schema_hash     = :schema_hash
            WHERE run_id = :run_id
        """), {
            "run_id":         run_id,
            "status":         status,
            "rows_received":  stats.get("rows_received"),
            "rows_loaded":    stats.get("rows_loaded"),
            "rows_rejected":  stats.get("rows_rejected"),
            "wells_expected": stats.get("wells_expected"),
            "coverage_pct":   stats.get("coverage_pct"),
            "null_rate_pct":  stats.get("null_rate_pct"),
            "failure_mode":   failure_mode,
            "error_message":  error,
            "schema_hash":    stats.get("schema_hash"),
        })


def run_pipeline(failure_mode: FailureMode = FailureMode.NONE):
    report_date   = date.today() - timedelta(days=1)
    source_file   = f"daily_prod_{report_date.strftime('%Y%m%d')}.csv"
    pipeline_name = "daily_production_ingest"
    print(f"\n{'='*60}\n  Pipeline : {pipeline_name}\n  Mode     : {failure_mode.value}\n{'='*60}")
    run_id = open_run(pipeline_name, source_file)
    print(f"  Run ID   : {run_id}")
    try:
        wells_expected = len(get_active_api_numbers())
        df = generate_production_report(report_date)
        if failure_mode != FailureMode.NONE:
            df = INJECTORS[failure_mode](df)
            print(f"  Injected : {failure_mode.value}")
        stats = {
            "rows_received":  len(df),
            "wells_expected": wells_expected,
            "schema_hash":    schema_hash(df),
            "null_rate_pct":  round(df["oil_bbls"].isna().mean() * 100, 2) if "oil_bbls" in df.columns else None,
            "coverage_pct":   round(len(df) / wells_expected * 100, 2),
        }
        validation = validate(df, wells_expected)
        stats["rows_rejected"] = len(df) if not validation.passed else 0
        stats["rows_loaded"]   = len(df) if validation.passed else 0
        if not validation.passed:
            for err in validation.errors:
                print(f"  ERROR: {err}")
            close_run(run_id, "FAILED", stats, failure_mode.value, error=json.dumps(validation.errors))
            print(f"\n  FAILED — run_id={run_id}\n")
            return run_id, False
        staging_cols = [c for c in EXPECTED_SCHEMA if c in df.columns]
        df_stage = df[staging_cols].copy()
        df_stage["source_file"] = source_file
        df_stage["is_valid"]    = None
        df_stage["validation_notes"] = None
        df_stage.to_sql("daily_production", con=engine, schema="stg",
                        if_exists="append", index=False, method="multi", chunksize=200)
        stats["rows_loaded"] = len(df)
        close_run(run_id, "SUCCESS", stats, failure_mode.value)
        print(f"  SUCCESS — {len(df)} rows loaded — run_id={run_id}\n")
        return run_id, True
    except Exception as exc:
        close_run(run_id, "FAILED", {}, failure_mode.value, error=str(exc))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--failure-mode", choices=[m.value for m in FailureMode], default="none")
    args = parser.parse_args()
    run_pipeline(FailureMode(args.failure_mode))
