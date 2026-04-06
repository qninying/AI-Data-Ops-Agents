from typing import Optional, List, Dict, Any
import streamlit as st
import pandas as pd
import requests
import sys
import os

# Ensure the parent directory is in the path to allow relative imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import run_query

st.set_page_config(
    page_title="Oilfield DataOps",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS for a premium glassmorphism light theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Fira+Code:wght@400;600&display=swap');

    p, span, div, h1, h2, h3, h4, label {
        font-family: 'Inter', sans-serif;
    }
    
    /* Protect Material Icons from font overrides */
    .material-icons, [data-testid="stIconMaterial"], span:empty {
        font-family: 'Material Symbols Rounded', 'Material Icons', sans-serif !important;
    }

    code, pre {
        font-family: 'Fira Code', monospace !important;
    }
    .stApp {
        background-color: transparent !important;
    }
    .metric-card {
        background: rgba(255, 255, 255, 0.85);
        border: 1px solid rgba(226, 232, 240, 0.8);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
        backdrop-filter: blur(12px);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 24px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border: 1px solid rgba(59, 130, 246, 0.5);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 0 20px rgba(59, 130, 246, 0.1);
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin-top: 8px;
        letter-spacing: -0.03em;
    }
    .metric-label {
        color: #64748b;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
    }
    .status-badge {
        padding: 6px 12px;
        border-radius: 6px;
        font-size: 0.85rem;
        font-weight: 700;
        display: inline-block;
        margin-bottom: 12px;
        letter-spacing: 0.5px;
    }
    .status-success { background: rgba(16, 185, 129, 0.1); color: #059669; border: 1px solid rgba(16, 185, 129, 0.3); }
    .status-failed { background: rgba(239, 68, 68, 0.1); color: #dc2626; border: 1px solid rgba(239, 68, 68, 0.3); }
    .status-partial { background: rgba(245, 158, 11, 0.1); color: #d97706; border: 1px solid rgba(245, 158, 11, 0.3); }
    
    h1, h2, h3 {
        letter-spacing: -0.02em;
    }
</style>
""", unsafe_allow_html=True)

st.title("Autonomous DataOps")
st.markdown("Real-time telemetry and AI self-healing pipeline for Oklahoma field operations.")
st.markdown("---")

def fetch_runs() -> pd.DataFrame:
    data: List[Dict[str, Any]] = run_query("SELECT run_id, run_start, run_end, status, rows_received, rows_loaded, coverage_pct, null_rate_pct, failure_mode, error_message, ai_incident_report, ai_fix_sql FROM dbo.etl_run_log ORDER BY run_id DESC")
    return pd.DataFrame(data)

try:
    runs_df = fetch_runs()
except Exception as e:
    st.error(f"Could not connect to Database: {e}")
    runs_df = pd.DataFrame()

tab1, tab2 = st.tabs(["📊 Telemetry Dashboard", "🧠 LangGraph Architecture"])

with tab2:
    st.markdown("""
```mermaid
flowchart TD
    classDef ext fill:#4F46E5,stroke:#3730A3,stroke-width:2px,color:white
    classDef api fill:#059669,stroke:#047857,stroke-width:2px,color:white
    classDef ai fill:#D97706,stroke:#B45309,stroke-width:2px,color:white
    classDef core fill:#475569,stroke:#334155,stroke-width:2px,color:white
    classDef db fill:#0284C7,stroke:#0369A1,stroke-width:2px,color:white

    subgraph UI ["User Experience"]
        Dashboard["Streamlit Dashboard (Light Mode)"]:::ext
    end

    subgraph Backend ["Middleware"]
        Webhook["FastAPI /analyze Webhook"]:::api
    end

    subgraph StateMachine ["⛓️ LangGraph Execution Engine"]
        direction TB
        Analyzer("🧠 Analyzer Agent<br><small>JSON Evaluator</small>"):::ai
        RootCause("🧠 Root Cause Agent<br><small>Hypothesis Builder</small>"):::ai
        Remediation("⚙️ Remediation Mapper<br><small>Native Python Payload</small>"):::core
        Executor("⚙️ SQL Executor<br><small>Transaction Isolation</small>"):::core
        Reporter("🧠 Incident Reporter<br><small>Markdown Writer</small>"):::ai
        
        Analyzer -->|"Failure Confirmed"| RootCause
        RootCause --> Remediation
        Remediation --> Executor
        Executor -->|"Validation Passed"| Reporter
    end

    subgraph DBTier ["Persistent Telemetry"]
        SQL[(Docker SQL Server 2022)]:::db
    end

    Dashboard -->|"1. Target Dead Run"| Webhook
    Webhook -->|"2. Spark Graph State"| Analyzer
    
    Analyzer -.->|"Reads Base Log"| SQL
    RootCause -.->|"Extracts Dynamic Evidence"| SQL
    Executor -.->|"Mutates and Verifies State"| SQL
    Reporter -.->|"Pushes Report Memory natively"| SQL
    
    Reporter -->|"3. Graph Return Vector"| Webhook
    Webhook -->|"4. Render T-SQL Blocks"| Dashboard
```
    """)

with tab1:
    if not runs_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-label">Total Pipeline Runs</div><div class="metric-value" style="color: #0f172a;">{len(runs_df)}</div></div>', unsafe_allow_html=True)
        with col2:
            failed: int = len(runs_df[runs_df['status'] == 'FAILED'])
            st.markdown(f'<div class="metric-card"><div class="metric-label">Failed Runs</div><div class="metric-value" style="color: #ef4444;">{failed}</div></div>', unsafe_allow_html=True)
        with col3:
            avg_coverage: float = runs_df['coverage_pct'].mean() if not runs_df['coverage_pct'].isnull().all() else 0.0
            st.markdown(f'<div class="metric-card"><div class="metric-label">Avg Coverage</div><div class="metric-value" style="color: #10b981;">{avg_coverage:.1f}%</div></div>', unsafe_allow_html=True)
        with col4:
            avg_nulls: float = runs_df['null_rate_pct'].mean() if not runs_df['null_rate_pct'].isnull().all() else 0.0
            st.markdown(f'<div class="metric-card"><div class="metric-label">Avg Null Rate</div><div class="metric-value" style="color: #f59e0b;">{avg_nulls:.1f}%</div></div>', unsafe_allow_html=True)
        
        st.subheader("Pipeline Telemetry")
        
        for _, row in runs_df.iterrows():
            status_lower = str(row['status']).lower()
            badge_class = f"status-{status_lower}" if status_lower in ['success', 'failed', 'partial'] else "status-partial"
            
            with st.expander(f"Run {row['run_id']}  |  {row['run_start']}", expanded=(row['status'] == 'FAILED')):
                st.markdown(f'<span class="status-badge {badge_class}">STATUS: {row["status"]}</span>', unsafe_allow_html=True)
                c1, c2, c3 = st.columns([2, 2, 1])
                c1.markdown(f"**Rows Received:** {row['rows_received']}<br>**Rows Loaded:** {row['rows_loaded']}", unsafe_allow_html=True)
                c2.markdown(f"**Failure Mode:** {row['failure_mode'] or 'N/A'}<br>**Coverage:** {row['coverage_pct']}%", unsafe_allow_html=True)
                
                if row['status'] == 'FAILED':
                    if row['error_message']:
                        st.error(f"Error: {row['error_message']}")
                    
                    # Check for existing report
                    if pd.notna(row.get('ai_incident_report')) and row['ai_incident_report']:
                        st.success("AI Sequence Completed (Historical Data)")
                        st.markdown("### Incident Report")
                        st.markdown(row['ai_incident_report'])
                        if pd.notna(row.get('ai_fix_sql')) and row['ai_fix_sql']:
                            st.markdown("### Executed SQL Remediation")
                            st.code(row['ai_fix_sql'], language="sql")
                    else:
                        if c3.button("Diagnose & Heal (AI Agents)", key=f"fix_{row['run_id']}", use_container_width=True, type="primary"):
                            with st.spinner("LangGraph multi-agent team analyzing & remediating..."):
                                try:
                                    resp = requests.post("http://localhost:8000/analyze", json={"run_id": int(row['run_id'])})
                                    if resp.status_code == 200:
                                        result: Dict[str, Any] = resp.json()
                                        st.success("AI Sequence Completed!")
                                        if result.get("incident_report"):
                                            st.markdown("### Incident Report")
                                            st.markdown(result["incident_report"])
                                            
                                            if result.get("fix_sql"):
                                                st.markdown("### Executed SQL Remediation")
                                                st.code(result["fix_sql"], language="sql")
                                        else:
                                            st.json(result)
                                    else:
                                        st.error(f"Webhook Error: {resp.text}")
                                except requests.exceptions.ConnectionError:
                                    st.warning("FastAPI server is unreachable at http://localhost:8000. Please start the backend.")
    else:
        st.info("No runs found in the database. Are you sure the ETL pipeline has been executed?")
