from __future__ import annotations
from datetime import datetime
from agents.state import DataOpsState

from database.db import execute_dml

def reporter_agent(state: DataOpsState) -> DataOpsState:
    print(f"\n[Reporter] Generating incident report for Run {state['run_id']}...")
    report = f"""## Incident Report — run_id={state['run_id']}
**Date:** {datetime.utcnow().strftime('%Y-%m-%d')}
**Pipeline:** {state.get('pipeline_name')}
**Failure Type:** {state.get('failure_type')}
**Severity:** {state.get('severity')}

### What Happened
{state.get('monitor_summary')}

### Root Cause
{state.get('hypothesis')}

### Fix Applied
{state.get('fix_description')}

### Validation
{state.get('executor_notes', 'Validation passed.')}
"""
    print("[Reporter] Report generated successfully. Persisting to database...")
    
    # Persist the report directly to the Database so the UI can retrieve it dynamically
    run_id = int(state['run_id'])
    fix_sql = state.get("fix_sql")
    try:
        execute_dml(
            "UPDATE dbo.etl_run_log SET ai_incident_report = :report, ai_fix_sql = :fix WHERE run_id = :r",
            {"report": report, "fix": fix_sql, "r": run_id}
        )
        print("[Reporter] Persisted to SQL Server successfully.\n")
    except Exception as e:
        print(f"[Reporter] ERROR persisting to SQL Server: {e}\n")

    return {
        **state, # type: ignore
        "incident_report": report,
        "messages": state["messages"] + [{"role": "reporter", "content": report}]
    }
