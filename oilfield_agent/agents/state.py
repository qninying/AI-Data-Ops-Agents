from __future__ import annotations
from typing import TypedDict, Optional, Annotated, List
from enum import Enum
import operator


class FailureType(str, Enum):
    SCHEMA_DRIFT   = "schema_drift"
    NULL_EXPLOSION  = "null_explosion"
    ROW_COUNT_DROP  = "row_count_drop"
    TYPE_MISMATCH   = "type_mismatch"
    UNKNOWN         = "unknown"
    NONE            = "none"


class Severity(str, Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"


class DataOpsState(TypedDict):
    # Run metadata
    run_id:          str
    pipeline_name:   str
    triggered_at:    str
    # Monitor
    raw_log:             str
    failure_detected:    bool
    monitor_summary:     Optional[str]
    # Classifier
    failure_type:        Optional[str]
    severity:            Optional[str]
    classifier_notes:    Optional[str]
    confidence:          Optional[float]
    # Root cause
    evidence:            Optional[dict]
    hypothesis:          Optional[str]
    affected_columns:    Optional[List[str]]
    # Remediation
    fix_sql:             Optional[str]
    fix_description:     Optional[str]
    rollback_sql:        Optional[str]
    # Executor
    fix_applied:         bool
    validation_passed:   bool
    retry_count:         int
    validation_details:  Optional[dict]
    executor_notes:      Optional[str]
    # Reporter
    incident_report:     Optional[str]
    escalate_to_human:   bool
    # Shared conversation history
    messages: Annotated[List[dict], operator.add]


def initial_state(run_id: str, pipeline_name: str, triggered_at: str) -> DataOpsState:
    return DataOpsState(
        run_id=run_id, pipeline_name=pipeline_name, triggered_at=triggered_at,
        raw_log="", failure_detected=False, monitor_summary=None,
        failure_type=None, severity=None, classifier_notes=None, confidence=None,
        evidence=None, hypothesis=None, affected_columns=None,
        fix_sql=None, fix_description=None, rollback_sql=None,
        fix_applied=False, validation_passed=False, retry_count=0,
        validation_details=None, executor_notes=None,
        incident_report=None, escalate_to_human=False, messages=[],
    )
