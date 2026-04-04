from __future__ import annotations
from typing import Optional, Dict, Any
from agents.state import DataOpsState, FailureType
from database.db import get_db
from sqlalchemy import text

def executor_agent(state: DataOpsState) -> DataOpsState:
    print("\n[Executor] Executing fix inside transaction...")
    fix_sql = state.get("fix_sql")
    run_id = state.get("run_id")
    retry_count = state.get("retry_count", 0)
    failure_type = state.get("failure_type")
    
    if not fix_sql or fix_sql.startswith("-- Error"):
        print("[Executor] Invalid fix_sql. Failing validation.")
        return {
            **state, # type: ignore
            "fix_applied": False,
            "validation_passed": False,
            "retry_count": retry_count + 1,
            "executor_notes": "No valid fix_sql provided.",
        }
        
    try:
        with get_db() as db:
            db.execute(text(fix_sql))
            validation_passed = True
            executor_notes = "Fix applied successfully."

            # Post-fix Hardcoded Validation
            try:
                if failure_type == FailureType.TYPE_MISMATCH:
                    bad_count = db.execute(text("SELECT COUNT(*) FROM stg.daily_production WHERE CAST(gas_mcf AS VARCHAR(MAX)) LIKE '% MCF%'")).scalar()
                    if bad_count and bad_count > 0:
                        validation_passed = False
                        executor_notes = f"Validation failed: {bad_count} rows still have ' MCF'."
                
                elif failure_type == FailureType.NULL_EXPLOSION:
                    bad_count = db.execute(text("SELECT COUNT(*) FROM stg.daily_production WHERE oil_bbls IS NULL AND (is_valid IS NULL OR is_valid = 1)")).scalar()
                    if bad_count and bad_count > 0:
                        validation_passed = False
                        executor_notes = f"Validation failed: {bad_count} null rows are not flagged."

                elif failure_type == FailureType.SCHEMA_DRIFT:
                    bad_count = db.execute(text("SELECT COUNT(*) FROM stg.daily_production")).scalar()
                    if bad_count and bad_count > 0:
                        validation_passed = False
                        executor_notes = f"Validation failed: Staging table is not empty ({bad_count} rows)."

                elif failure_type == FailureType.ROW_COUNT_DROP:
                    status = db.execute(text("SELECT status FROM dbo.etl_run_log WHERE run_id = :r"), {"r": run_id}).scalar()
                    if status != 'PARTIAL':
                        validation_passed = False
                        executor_notes = f"Validation failed: Logging status is {status}, not PARTIAL."
                        
            except Exception as val_e:
                validation_passed = False
                executor_notes = f"Validation error during SQL checks: {val_e}"

    except Exception as e:
        print(f"[Executor] Exception during execution: {e}")
        validation_passed = False
        executor_notes = str(e)
        
    return {
        **state, # type: ignore
        "fix_applied": validation_passed,
        "validation_passed": validation_passed,
        "retry_count": retry_count + (1 if not validation_passed else 0),
        "executor_notes": executor_notes,
        "messages": state["messages"] + [{"role": "executor", "content": executor_notes}]
    }
