from __future__ import annotations
from typing import Optional
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.schema import HumanMessage, SystemMessage
from agents.state import DataOpsState
from database.db import run_query, run_scalar
from config.settings import settings


def get_llm():
    if settings.llm_provider == "gemini":
        return ChatGoogleGenerativeAI(model="gemini-2.5-flash",
                               google_api_key=settings.google_api_key, temperature=0)
    if settings.llm_provider == "anthropic":
        return ChatAnthropic(model=settings.llm_model,
                             anthropic_api_key=settings.anthropic_api_key, temperature=0)
    return ChatOpenAI(model=settings.llm_model,
                      openai_api_key=settings.openai_api_key, temperature=0)


def fetch_latest_run(run_id: str) -> Optional[dict]:
    rows = run_query("""
        SELECT run_id, pipeline_name, run_start, run_end, status, source_file,
               rows_received, rows_loaded, rows_rejected, wells_expected,
               coverage_pct, null_rate_pct, failure_mode, error_message, schema_hash
        FROM dbo.etl_run_log WHERE run_id = :run_id
    """, {"run_id": int(run_id)})
    return rows[0] if rows else None


def fetch_recent_history(pipeline_name: str) -> list:
    return run_query("""
        SELECT TOP 5 run_id, status, rows_received, coverage_pct,
               null_rate_pct, failure_mode, run_start
        FROM dbo.etl_run_log WHERE pipeline_name = :pipeline_name
        ORDER BY run_id DESC
    """, {"pipeline_name": pipeline_name})


def format_run_for_llm(run: dict, history: list) -> str:
    lines = [
        "=== CURRENT ETL RUN ===",
        f"Run ID        : {run['run_id']}",
        f"Pipeline      : {run['pipeline_name']}",
        f"Status        : {run['status']}",
        f"Source file   : {run['source_file']}",
        f"Rows received : {run['rows_received']}",
        f"Wells expected: {run['wells_expected']}",
        f"Coverage %    : {run['coverage_pct']}%",
        f"Null rate %   : {run['null_rate_pct']}%",
        f"Error         : {run['error_message'] or 'None'}",
        "", "=== RECENT HISTORY ===",
    ]
    for h in history:
        lines.append(f"  run={h['run_id']} | {h['status']} | coverage={h['coverage_pct']}% | "
                     f"nulls={h['null_rate_pct']}% | mode={h['failure_mode']}")
    return "\n".join(lines)


import json
from agents.state import FailureType, Severity

SYSTEM_PROMPT = """You are the Advanced Monitor Analyst for an Oklahoma oilfield daily production ETL pipeline.
Review the run log and definitively classify the issue.

Failure Types: schema_drift, null_explosion, row_count_drop, type_mismatch, none, unknown
Severities: low, medium, high, critical

Respond ONLY with valid JSON, no markdown, no backticks:
{
  "failure_detected": true,
  "monitor_summary": "3-5 sentence summary of what went right/wrong.",
  "failure_type": "schema_drift",
  "severity": "high",
  "confidence": 0.92
}"""

def monitor_agent(state: DataOpsState) -> DataOpsState:
    print(f"\n[Monitor] Analyzing run_id={state['run_id']}...")
    run = fetch_latest_run(state["run_id"])
    if not run:
        return {**state, "failure_detected": False,
                "monitor_summary": "No run found",
                "messages": state["messages"] + [{"role": "monitor", "content": "No run found"}]} # type: ignore

    history          = fetch_recent_history(run["pipeline_name"])
    raw_log          = format_run_for_llm(run, history)
    
    llm      = get_llm()
    response = llm.invoke([SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=raw_log)])
    
    try:
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw.strip())
        
        failure_detected = result.get("failure_detected", run["status"] in ("FAILED", "PARTIAL"))
        summary = result.get("monitor_summary", "Analyzed run log.")
        failure_type = result.get("failure_type", FailureType.UNKNOWN)
        severity = result.get("severity", Severity.MEDIUM)
        confidence = float(result.get("confidence", 0.5))
    except Exception as e:
        failure_detected = run["status"] in ("FAILED", "PARTIAL")
        summary = f"Error evaluating LLM state: {e}"
        failure_type = FailureType.UNKNOWN
        severity = Severity.MEDIUM
        confidence = 0.0

    print(f"[Monitor] Detected: {failure_detected} | Type: {failure_type} | Conf: {confidence:.0%}")

    return {
        **state, # type: ignore
        "raw_log": raw_log,
        "failure_detected": failure_detected,
        "monitor_summary": summary,
        "failure_type": failure_type,
        "severity": severity,
        "confidence": confidence,
        "classifier_notes": summary,
        "messages": state["messages"] + [{"role": "monitor", "content": summary}]
    }
