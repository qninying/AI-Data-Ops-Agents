from __future__ import annotations
import json
from langchain.schema import HumanMessage, SystemMessage
from agents.state import DataOpsState, FailureType
from agents.monitor import get_llm
from database.db import run_query, run_scalar


def gather_schema_drift_evidence(run_id: str) -> dict:
    current      = run_query("SELECT schema_hash, error_message FROM dbo.etl_run_log WHERE run_id=:r", {"r": int(run_id)})
    last_success = run_query("SELECT TOP 1 schema_hash, run_id FROM dbo.etl_run_log WHERE status='SUCCESS' AND run_id<:r ORDER BY run_id DESC", {"r": int(run_id)})
    return {
        "current_hash":       current[0]["schema_hash"] if current else None,
        "last_success_hash":  last_success[0]["schema_hash"] if last_success else None,
        "hash_changed":       current[0]["schema_hash"] != last_success[0]["schema_hash"] if current and last_success else None,
        "error_message":      current[0]["error_message"] if current else None,
    }


def gather_null_explosion_evidence(run_id: str) -> dict:
    trend       = run_query("SELECT TOP 5 run_id, null_rate_pct, status FROM dbo.etl_run_log ORDER BY run_id DESC")
    source_file = run_scalar("SELECT source_file FROM dbo.etl_run_log WHERE run_id=:r", {"r": int(run_id)})
    null_count  = run_scalar("SELECT COUNT(*) FROM stg.daily_production WHERE source_file=:f AND oil_bbls IS NULL", {"f": source_file}) if source_file else 0
    total_count = run_scalar("SELECT COUNT(*) FROM stg.daily_production WHERE source_file=:f", {"f": source_file}) if source_file else 0
    field_dist  = run_query("""
        SELECT f.field_name, f.county, COUNT(*) as null_count
        FROM stg.daily_production sp
        JOIN dbo.wells w ON sp.api_number=w.api_number
        JOIN dbo.oil_fields f ON w.field_id=f.field_id
        WHERE sp.source_file=:f AND sp.oil_bbls IS NULL
        GROUP BY f.field_name, f.county ORDER BY null_count DESC
    """, {"f": source_file}) if source_file else []
    return {
        "null_count": null_count, "total_rows": total_count,
        "null_rate":  round(null_count / total_count * 100, 2) if total_count else 0,
        "trend":      [{"run_id": r["run_id"], "null_rate_pct": str(r["null_rate_pct"])} for r in trend],
        "by_field":   [{"field": r["field_name"], "county": r["county"], "count": r["null_count"]} for r in field_dist],
    }


def gather_row_count_drop_evidence(run_id: str) -> dict:
    run = run_query("SELECT rows_received, wells_expected, coverage_pct, source_file FROM dbo.etl_run_log WHERE run_id=:r", {"r": int(run_id)})
    if not run:
        return {}
    source_file   = run[0]["source_file"]
    reported      = {r["api_number"] for r in run_query("SELECT api_number FROM stg.daily_production WHERE source_file=:f", {"f": source_file})}
    all_active    = run_query("SELECT w.api_number, w.well_name, f.field_name, f.county FROM dbo.wells w JOIN dbo.oil_fields f ON w.field_id=f.field_id WHERE w.well_type='PRODUCER' AND w.status='ACTIVE'")
    missing       = [{"api": w["api_number"], "well": w["well_name"], "field": w["field_name"], "county": w["county"]} for w in all_active if w["api_number"] not in reported]
    county_counts: dict = {}
    for w in missing:
        county_counts[w["county"]] = county_counts.get(w["county"], 0) + 1
    return {
        "rows_received": run[0]["rows_received"], "wells_expected": run[0]["wells_expected"],
        "coverage_pct":  str(run[0]["coverage_pct"]), "missing_count": len(missing),
        "missing_sample": missing[:5], "missing_by_county": county_counts,
    }


def gather_type_mismatch_evidence(run_id: str) -> dict:
    source_file = run_scalar("SELECT source_file FROM dbo.etl_run_log WHERE run_id=:r", {"r": int(run_id)})
    error_msg   = run_scalar("SELECT error_message FROM dbo.etl_run_log WHERE run_id=:r", {"r": int(run_id)})
    sample      = run_query("SELECT TOP 5 api_number, oil_bbls, gas_mcf, water_bbls FROM stg.daily_production WHERE source_file=:f", {"f": source_file}) if source_file else []
    return {"error_message": error_msg, "source_file": source_file,
            "sample": [{k: str(v) for k, v in r.items()} for r in sample]}


EVIDENCE_GATHERERS = {
    FailureType.SCHEMA_DRIFT:   gather_schema_drift_evidence,
    FailureType.NULL_EXPLOSION:  gather_null_explosion_evidence,
    FailureType.ROW_COUNT_DROP:  gather_row_count_drop_evidence,
    FailureType.TYPE_MISMATCH:   gather_type_mismatch_evidence,
}

SYSTEM_PROMPT = """You are the Root Cause Analyst for an Oklahoma oilfield production pipeline.
Analyze the database evidence and form a specific root cause hypothesis.
Reference actual field names, counties, and metrics from the evidence.

Respond ONLY with valid JSON:
{
  "hypothesis": "2-3 sentence specific root cause statement",
  "affected_columns": ["col1", "col2"],
  "confidence": 0.88,
  "additional_notes": "Extra context"
}"""


def root_cause_agent(state: DataOpsState) -> DataOpsState:
    failure_type = state.get("failure_type", FailureType.UNKNOWN)
    print(f"\n[RootCause] Gathering evidence for {failure_type}...")
    gatherer = EVIDENCE_GATHERERS.get(failure_type)
    evidence = gatherer(state["run_id"]) if gatherer else {"note": "Unknown failure type"}
    content  = f"FAILURE TYPE: {failure_type}\nSEVERITY: {state.get('severity')}\n\nEVIDENCE:\n{json.dumps(evidence, indent=2, default=str)}"
    llm      = get_llm()
    response = llm.invoke([SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=content)])
    try:
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result           = json.loads(raw.strip())
        hypothesis       = result.get("hypothesis", "Could not determine")
        affected_columns = result.get("affected_columns", [])
        confidence       = float(result.get("confidence", 0.5))
    except (json.JSONDecodeError, KeyError) as e:
        hypothesis, affected_columns, confidence = f"Parse error: {e}", [], 0.0

    print(f"[RootCause] {hypothesis[:120]}...")
    return {**state, "evidence": evidence, "hypothesis": hypothesis,
            "affected_columns": affected_columns, "confidence": confidence,
            "messages": state["messages"] + [{"role": "root_cause", "content": hypothesis}]}
