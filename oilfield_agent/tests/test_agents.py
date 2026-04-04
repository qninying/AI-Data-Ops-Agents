"""
Test runner — works for all weeks.
Usage:
    python3 -m tests.test_agents --run-id 1      # test one run
    python3 -m tests.test_agents                 # test all runs
"""
from __future__ import annotations
import argparse
from database.db import run_query
from graph.data_ops_graph import run_pipeline_analysis


def print_summary(state: dict):
    print("\n── Agent Decisions ──────────────────────────────────────────")
    print(f"  Monitor    : {'FAILURE' if state['failure_detected'] else 'Healthy'}")
    print(f"               {(state.get('monitor_summary') or '')[:180]}")
    print(f"  Classifier : {state.get('failure_type')} | {state.get('severity')} | {(state.get('confidence') or 0):.0%}")
    print(f"               {(state.get('classifier_notes') or '')[:150]}")
    print(f"  Root Cause : {(state.get('hypothesis') or 'N/A')[:180]}")
    print(f"  Affected   : {state.get('affected_columns')}")
    print(f"  Remediation: {state.get('fix_description')}")
    print(f"  Executor   : Executed={state.get('fix_applied')} | Validated={state.get('validation_passed')} | Retries={state.get('retry_count')}")
    print(f"               {state.get('executor_notes')}")
    print(f"  Reporter   : Incident Report logic executed? {'Yes' if state.get('incident_report') else 'No'}")
    print(f"  Escalate   : {state.get('escalate_to_human')}")
    print("─────────────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, default=None)
    args = parser.parse_args()

    if args.run_id:
        state = run_pipeline_analysis(args.run_id)
        print_summary(state)
    else:
        runs = run_query("SELECT run_id, status, failure_mode FROM dbo.etl_run_log ORDER BY run_id")
        print(f"\nTesting {len(runs)} runs...\n")
        for run in runs:
            print(f"\n{'#'*60}")
            print(f"  run_id={run['run_id']} | {run['status']} | {run['failure_mode']}")
            state = run_pipeline_analysis(run["run_id"])
            print_summary(state)
