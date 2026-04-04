from __future__ import annotations
from agents.state import DataOpsState, FailureType

def remediation_agent(state: DataOpsState) -> DataOpsState:
    failure_type = state.get("failure_type")
    run_id = state.get("run_id")
    print(f"\n[Remediation] Generating ultra-fast native fix for {failure_type}...")
    
    if failure_type == FailureType.SCHEMA_DRIFT:
        fix_sql = f"DELETE FROM stg.daily_production;\nUPDATE dbo.etl_run_log SET status = 'RESOLVED', error_message='Schema drift mitigated' WHERE run_id = {run_id};"
        fix_desc = "Flushed staging table securely to prevent downstream schema drift propagation."
    elif failure_type == FailureType.NULL_EXPLOSION:
        fix_sql = f"UPDATE stg.daily_production SET is_valid = 0, validation_notes = 'Null value excluded' WHERE oil_bbls IS NULL;\nUPDATE dbo.etl_run_log SET status = 'PARTIAL', failure_mode='null_explosion_mitigated' WHERE run_id = {run_id};"
        fix_desc = "Isolated null propagation rows natively inside the database."
    elif failure_type == FailureType.ROW_COUNT_DROP:
        fix_sql = f"UPDATE dbo.etl_run_log SET status = 'PARTIAL', error_message = 'Missing wells identified and flagged' WHERE run_id = {run_id};"
        fix_desc = "Flagged pipeline row drop for further manual SCADA interpolation."
    elif failure_type == FailureType.TYPE_MISMATCH:
        fix_sql = f"UPDATE stg.daily_production SET gas_mcf = REPLACE(CAST(gas_mcf AS VARCHAR(MAX)), ' MCF', '') WHERE CAST(gas_mcf AS VARCHAR(MAX)) LIKE '% MCF%';\nUPDATE dbo.etl_run_log SET status = 'RESOLVED', error_message='Type mismatch resolved' WHERE run_id = {run_id};"
        fix_desc = "Cast gas_mcf natively to scrub 'MCF' strings from integer pools."
    else:
        fix_sql = f"-- No automated fix implemented for {failure_type}"
        fix_desc = "No valid fix generated."

    print(f"[Remediation] Proposed Fix: {fix_desc}")
    
    return {
        **state,  # type: ignore
        "fix_sql": fix_sql,
        "fix_description": fix_desc,
        "rollback_sql": "",
        "messages": state["messages"] + [{"role": "remediation", "content": fix_desc}]
    }
