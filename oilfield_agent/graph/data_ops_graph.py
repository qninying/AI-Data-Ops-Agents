from __future__ import annotations
from datetime import datetime
from langgraph.graph import StateGraph, END
from agents.state import DataOpsState, initial_state
from agents.monitor import monitor_agent

from agents.root_cause import root_cause_agent
from agents.remediation import remediation_agent
from agents.executor import executor_agent
from agents.reporter import reporter_agent
from config.settings import settings


# --- Conditional edges ---

def route_after_monitor(state: DataOpsState) -> str:
    if state["failure_detected"]:
        if (state.get("confidence") or 0) >= settings.confidence_threshold:
            print(f"[Graph] Failure detected & Confidence {state.get('confidence', 0):.0%} — routing to Root Cause")
            return "analyze"
        else:
            print(f"[Graph] Failure detected but low confidence — escalating")
            return "escalate"
    print("[Graph] No failure — pipeline healthy")
    return "end"


def route_after_executor(state: DataOpsState) -> str:
    if state["validation_passed"]:
        print("[Graph] Validation passed — routing to Reporter")
        return "report"
    elif state["retry_count"] < settings.max_retries:
        print(f"[Graph] Validation failed. Retrying... ({state['retry_count']}/{settings.max_retries})")
        return "remediate"
    else:
        print("[Graph] Max retries reached — escalating")
        return "escalate"


def escalate_node(state: DataOpsState) -> DataOpsState:
    print("[Graph] Escalating to human operator")
    return {**state, "escalate_to_human": True,  # type: ignore
            "messages": state["messages"] + [{"role": "system", "content": "Escalated: low confidence or max retries reached"}]}


# --- Build graph ---

def build_graph():
    g = StateGraph(DataOpsState)
    g.add_node("monitor",    monitor_agent)
    g.add_node("root_cause", root_cause_agent)
    g.add_node("remediation", remediation_agent)
    g.add_node("executor",   executor_agent)
    g.add_node("reporter",   reporter_agent)
    g.add_node("escalate",   escalate_node)

    g.set_entry_point("monitor")
    g.add_conditional_edges("monitor", route_after_monitor, {"analyze": "root_cause", "escalate": "escalate", "end": END})
    
    g.add_edge("root_cause", "remediation")
    g.add_edge("remediation", "executor")
    g.add_conditional_edges("executor", route_after_executor, {"report": "reporter", "remediate": "remediation", "escalate": "escalate"})
    
    g.add_edge("reporter", END)
    g.add_edge("escalate", END)
    return g.compile()


def run_pipeline_analysis(run_id: int, pipeline_name: str = "daily_production_ingest") -> DataOpsState:
    app   = build_graph()
    state = initial_state(str(run_id), pipeline_name, datetime.utcnow().isoformat())
    print(f"\n{'='*60}\n  Agent pipeline starting — run_id={run_id}\n{'='*60}")
    final = app.invoke(state)
    print(f"\n{'='*60}")
    print(f"  Failure detected : {final['failure_detected']}")
    print(f"  Failure type     : {final['failure_type']}")
    print(f"  Severity         : {final['severity']}")
    print(f"  Hypothesis       : {(final.get('hypothesis') or 'N/A')[:100]}")
    print(f"  Validation       : {final.get('validation_passed', False)}")
    print(f"  Escalate         : {final['escalate_to_human']}")
    print(f"{'='*60}")
    return final  # type: ignore
