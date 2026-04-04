from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sys
import os

# Ensure the parent directory is in the path to allow relative imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.data_ops_graph import run_pipeline_analysis

app = FastAPI(title="Oilfield DataOps API", version="1.0.0")

class AnalyzeRequest(BaseModel):
    run_id: int
    pipeline_name: str = "daily_production_ingest"

class AnalyzeResponse(BaseModel):
    run_id: str
    failure_detected: bool
    failure_type: Optional[str] = None
    severity: Optional[str] = None
    hypothesis: Optional[str] = None
    fix_applied: bool
    fix_sql: Optional[str] = None
    incident_report: Optional[str] = None
    escalate_to_human: bool

@app.post("/analyze", response_model=AnalyzeResponse)
def trigger_analysis(request: AnalyzeRequest) -> AnalyzeResponse:
    try:
        final_state = run_pipeline_analysis(request.run_id, request.pipeline_name)
        return AnalyzeResponse(
            run_id=final_state.get("run_id", str(request.run_id)),
            failure_detected=final_state.get("failure_detected", False),
            failure_type=final_state.get("failure_type"),
            severity=final_state.get("severity"),
            hypothesis=final_state.get("hypothesis"),
            fix_applied=final_state.get("fix_applied", False),
            fix_sql=final_state.get("fix_sql"),
            incident_report=final_state.get("incident_report"),
            escalate_to_human=final_state.get("escalate_to_human", False),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
