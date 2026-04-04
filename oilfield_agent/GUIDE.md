# AI Data Ops Agent — Oilfield Edition
## Complete Project Guide (Weeks 1–4)

---

## What We Are Building

A multi-agent AI system that monitors an Oklahoma oilfield daily production ETL pipeline.
When the pipeline fails, six specialized AI agents collaborate autonomously to:
1. Detect the failure
2. Classify what type it is
3. Reason about the root cause using real database evidence
4. Generate a fix
5. Execute and validate the fix
6. Write a structured incident report

**Stack:** LangGraph · LangChain · Anthropic API · SQL Server · SQLAlchemy · FastAPI · Streamlit

---

## Prerequisites (Already Installed)
- Homebrew
- Docker Desktop (running)
- Python 3.9
- sqlcmd
- ODBC Driver 17 for SQL Server
- unixodbc

---

## One-Time Setup

```bash
# 1. Navigate to project
cd ~/Desktop/oilfield_agent

# 2. Start SQL Server
docker compose up -d
# Wait 20 seconds

# 3. Apply schema
sqlcmd -S localhost,1434 -U sa -P 'StrongPass123' -i database/schema.sql -C

# 4. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 5. Configure environment
cp .env.example .env
# Open .env and add your ANTHROPIC_API_KEY

# 6. Verify connection
python3 -c "from database.db import run_scalar; print(run_scalar('SELECT DB_NAME()'))"
```

---

## Project Structure

```
oilfield_agent/
├── config/
│   └── settings.py          # App settings, DB connection string
├── database/
│   ├── schema.sql            # SQL Server DDL + 8 fields + 50 wells
│   └── db.py                 # SQLAlchemy engine + query helpers
├── etl/
│   └── pipeline.py           # Daily production ETL + 4 failure modes
├── agents/
│   ├── state.py              # LangGraph shared state TypedDict
│   ├── monitor.py            # Agent 1: reads etl_run_log
│   ├── classifier.py         # Agent 2: labels failure type
│   ├── root_cause.py         # Agent 3: queries DB for evidence
│   ├── remediation.py        # Agent 4: generates T-SQL fix       ← Week 3
│   ├── executor.py           # Agent 5: runs fix, validates        ← Week 3
│   └── reporter.py           # Agent 6: writes incident report     ← Week 4
├── graph/
│   └── data_ops_graph.py     # LangGraph state machine
├── api/
│   └── main.py               # FastAPI webhook trigger             ← Week 4
├── dashboard/
│   └── app.py                # Streamlit agent activity dashboard  ← Week 4
├── tests/
│   └── test_agents.py        # Test runner for all weeks
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Week 1 — Foundation (COMPLETE)

**Goal:** Working database, seeded data, ETL pipeline with 4 injectable failure modes.

### What was built:
- SQL Server 2022 in Docker on port 1434 (Apple Silicon compatible)
- `OilfieldOps` database with 8 Oklahoma fields and 50 wells
- `dbo.etl_run_log` — the table all agents read from
- ETL pipeline that generates realistic daily production data
- 4 failure modes that write FAILED records to etl_run_log

### Run the ETL pipeline:
```bash
python3 -m etl.pipeline                              # clean run
python3 -m etl.pipeline --failure-mode schema_drift
python3 -m etl.pipeline --failure-mode null_explosion
python3 -m etl.pipeline --failure-mode row_count_drop
python3 -m etl.pipeline --failure-mode type_mismatch
```

### The 4 failure modes:
| Mode | Real-world cause | What breaks |
|------|-----------------|-------------|
| schema_drift | Vendor adds NGL/condensate columns without notice | Schema contract |
| null_explosion | SCADA meter outage, 38% of oil readings go NULL | Data quality |
| row_count_drop | Radio tower failure, only 4 of 46 wells report | Coverage |
| type_mismatch | Vendor appends " MCF" to gas values | Type integrity |

### Verify:
```bash
python3 -c "
from database.db import run_query
rows = run_query('SELECT run_id, status, failure_mode, coverage_pct, null_rate_pct FROM dbo.etl_run_log ORDER BY run_id')
for r in rows: print(r)
"
```

---

## Week 2 — Monitor, Classifier, Root Cause (COMPLETE)

**Goal:** First 3 LangGraph agents wired together. AI starts reasoning about failures.

### What was built:
- `agents/monitor.py` — reads etl_run_log, asks LLM to summarize
- `agents/classifier.py` — LLM classifies failure type + severity + confidence
- `agents/root_cause.py` — runs targeted SQL queries, LLM forms hypothesis
- `graph/data_ops_graph.py` — LangGraph state machine with conditional routing

### How the graph routes:
```
Monitor → failure detected? → NO  → END (healthy)
                            → YES → Classifier
                                      ↓
                            confidence ≥ 75%? → YES → Root Cause → END
                                               → NO  → Escalate  → END
```

### Test the agents:
```bash
# Test clean run (should exit after Monitor with no failure)
python3 -m tests.test_agents --run-id 1

# Test a specific failure
python3 -m tests.test_agents --run-id 3

# Test all runs
python3 -m tests.test_agents
```

---

## Week 3 — Remediation + Executor (COMING)

**Goal:** Agents that actually fix the problem.

### What will be built:
- `agents/remediation.py` — takes the hypothesis, generates a T-SQL fix
- `agents/executor.py` — runs the fix inside a transaction, validates output
- Self-healing retry loop — if validation fails, sends state back to Remediation
- Guardrails — max 3 retries, always runs inside a transaction with rollback

### How each failure gets fixed:
| Failure | Fix strategy |
|---------|-------------|
| schema_drift | Truncate staging, reload with schema-safe columns only |
| null_explosion | Flag null rows in staging, reload from last clean source |
| row_count_drop | Identify missing wells, flag run as PARTIAL not FAILED |
| type_mismatch | Strip unit suffixes from gas_mcf, cast to DECIMAL, reload |

### The retry loop:
```
Root Cause → Remediation → Executor → validation passed? → YES → Reporter
                              ↑                           → NO  → retry < 3?
                              └───────────────────────────────────┘
```

---

## Week 4 — Reporter + API + Dashboard (COMING)

**Goal:** Complete system with incident reports, webhook trigger, and live dashboard.

### What will be built:
- `agents/reporter.py` — writes a structured Markdown incident report
- `api/main.py` — FastAPI endpoint to trigger the agent chain externally
- `dashboard/app.py` — Streamlit dashboard showing real-time agent activity

### Incident report output format:
```
## Incident Report — run_id=3
**Date:** 2026-04-01
**Pipeline:** daily_production_ingest
**Failure Type:** null_explosion
**Severity:** high

### What Happened
SCADA meter outage affected 17 wells across Kingfisher and Canadian counties...

### Root Cause
38% of oil_bbls readings returned NULL due to instrument communication failure...

### Fix Applied
Null rows flagged in staging. Clean rows promoted to dbo.daily_production...

### Validation
Loaded 28 of 46 wells (61% coverage). Remaining 18 flagged for manual review.
```

### API trigger:
```bash
curl -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -d '{"run_id": 3}'
```

---

## Key Concepts

**LangGraph state** — `DataOpsState` in `agents/state.py` is a TypedDict that flows
through every agent. Each agent reads from it and writes its outputs back.
The `messages` list uses `operator.add` so each agent appends without overwriting.

**Conditional edges** — `graph/data_ops_graph.py` uses conditional routing functions
that inspect the state and return a string key that maps to the next node.
This is what makes the graph intelligent — it doesn't always follow the same path.

**Evidence-driven reasoning** — the Root Cause agent doesn't just ask the LLM to guess.
It first queries the actual database (which fields have nulls, which wells didn't report,
whether the schema hash changed) and passes that concrete evidence to the LLM.
This is the difference between a useful AI agent and a hallucinating one.

**JSON-forced outputs** — Classifier and Root Cause agents use system prompts that
force the LLM to respond in strict JSON only. This makes outputs parseable and
deterministic enough to drive downstream logic.
