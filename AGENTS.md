# AGENTS.md

Operational guide for integrating AI coding/analysis agents (e.g. OpenAI Codex / GPT Assistants / multi-agent orchestrators) with the Penguinarium ("Tupik") repository.

## 1. Purpose & Philosophy
Tupik is a 3‑stage data quality & anomaly intelligence platform:
1. Statistical profiling & divergence detection.
2. AI/ML context‑aware anomaly reasoning.
3. Agentic root‑cause exploration (this document enables Stage 3 automation).

Agents must:
- Be deterministic / reproducible (transparent diffs, idempotent actions where possible).
- Minimize hallucination: prefer reading files before assuming structure.
- Uphold safety (no secrets leakage, no destructive bulk edits without justification).
- Enforce quality gates before proposing changes.

Output hierarchy: (a) Correctness (b) Clarity (c) Minimality (d) Extensibility.

## 2. Repository High‑Level Map (For System Prompts)
Backend (Django 5 + DRF) `backend/src`:
- `config/` settings, ASGI/WSGI, env-based Postgres config.
- `api/` REST endpoints, serializers (`serializers/`), pagination, permissions, exceptions, logging middleware.
- `jobs/`, `pulling/`, `dagster/` Django apps for domain-specific logic.
- `common/` shared models/utilities (currently `models.py`).
- `manage.py` admin/management entrypoint.

Dagster pipelines `dagster_app/`:
- `dagster_app/jobs/*` job definitions (metadata & statistics extraction).
- `dagster_app/ops/*` low-level ops (dataset ingestion, transforms).
- `dagster_app/utils/*` helpers (dataset IO, statistics, persistence, metadata composition).
- `dagster_home/dagster.yaml` Dagster instance config (uses Postgres via env vars in compose).

Frontend (Streamlit) `frontend/`:
- `main.py`, `pages/*.py` provide dashboard, alerts, datasource navigation.
- `api_client.py` couples to backend (`API_HOST`, `API_PORT`).

Orchestration & Infra:
- `compose.yaml` defines services: `tupik` (Django API), `frontend`, `db` (Postgres), `dagster_app`, `adminer`.
- `docs/` contains statistical & AI/ML method specs (reference for generating new checks / metadata handlers).
- CSV datasets under `dagster_app/data/home_credit/` consumed by Dagster ops.

## 3. Environments & Tooling
Python versions:
- Backend & Frontend specify `requires-python >=3.13` (bleeding edge CPython).
- Dagster app specifies `>=3.10` (stable). In Docker you may pin distinct base images; DO NOT unify without validating Dagster compatibility with 3.13.

Dependency Managers:
- Backend & frontend appear to use `uv` (lock in `uv.lock`).
- Dagster uses `hatchling` build backend.

Containers (preferred execution):
- Use `docker compose up -d --build` after setting a `.env` file (see §4). Avoid running migrations outside containers unless explicitly required.

Local (optional):
- Prefer isolated virtual env per logical component if running outside Docker.
- Keep parity with Dockerfile base image versions.

Testing:
- Backend: `pytest` + `pytest-django`. Settings override likely via `DJANGO_SETTINGS_MODULE=config.settings_test` (not yet auto-wired—agent must confirm before using).
- Dagster: limited tests under `dagster_app/tests`. Extend for new ops/jobs.
- Add tests BEFORE large refactors.

Performance / Data Size:
- Home Credit CSVs may be large; stream or chunk where possible (pandas `iterator=True`, consider profiling if >100MB processed).

## 4. Environment Variables (Authoritative)
Backend / Compose usage (with defaults):
- `POSTGRES_DB` (postgres)
- `POSTGRES_USER` (postgres)
- `POSTGRES_PASSWORD` (postgres)
- `POSTGRES_HOST` (localhost or service `db` in compose)
- `POSTGRES_PORT` (5432)
- `DJANGO_PORT` container internal (exposed as 8000:DJANGO_PORT)
- `API_HOST` used by frontend to form base URL; also mapped to `ALLOWED_HOST` in backend.
- Optional flags: `DJANGO_ALLOWED_HOSTS`, `ALLOW_ALL_HOSTS`, `DJANGO_ALLOW_ALL_HOSTS`.

Dagster:
- `DATASET_DIR` (mounted path for datasets)
- `DAGSTER_POSTGRES_*` parallel to backend DB env vars (shared Postgres).

Agent Rules:
- NEVER commit real secrets (rotate immediately if leaked).
- If generating new settings referencing env vars, document them in this section.
- Validate presence via `os.getenv` checks with safe fallbacks.

## 5. Agent Roles & Responsibilities
1. Code Implementation Agent
   - Adds endpoints, models, serializers, Dagster ops/jobs. Reads existing patterns first.
2. Data Pipeline Agent
   - Extends dataset ingestion, metadata, statistics calculation. Must reference docs in `docs/`.
3. Analysis & Root-Cause Agent
   - Consumes statistics + metadata JSON under `dagster_app/storage/*` to hypothesize anomalies; outputs structured YAML (see §11 suggestions) for future automation.
4. Testing & Quality Agent
   - Ensures test coverage additions; can scaffold missing tests for new code.
5. Documentation Agent
   - Updates `README.md`, `docs/*.md`, adds inline docstrings and usage examples.
6. Refactor / Safety Agent (Gatekeeper)
   - Final reviewer: checks diff scope, ensures migrations generated if model changes, verifies idempotency.

Escalation Flow (suggested multi-agent pipeline):
User Request -> Planner (decompose) -> Implementer -> Tester -> Refactor/Gatekeeper -> Doc Updater -> Final Answer.

## 6. Workflow Protocol
1. Clarify: Re-state task & acceptance criteria in <5 bullets.
2. Discover: Read impacted files (avoid assumptions). If unknown symbol, search first.
3. Design: Provide concise diff plan (group by file) before large edits.
4. Implement: Minimal patch; avoid formatting unrelated lines.
5. Validate: Run tests / migrations (dry-run). Add new tests.
6. Review: Self-checklist (§10) then produce final answer containing: summary, changed files list, follow-ups.
7. Commit Message Template (§8).

## 7. Coding & Architectural Standards
General:
- Type hints mandatory for new Python functions (PEP 484).
- Keep functions short; extract helpers if >40 LOC or 3+ responsibilities.
- Avoid global state; prefer dependency injection via function params or class init.

Django / DRF:
- Serializer <-> Model naming consistency (`ModelNameSerializer`).
- Use DRF pagination & filtering infrastructure; centralize filters.
- When adding a model: create migrations (`python manage.py makemigrations <app>` inside container), include verbose_name/meta ordering if meaningful.
- API views: prefer class-based views / viewsets; keep business logic out of views (move to services or model methods).
- Exceptions: use `api.exceptions.api_exception_handler` path; map custom exceptions to structured JSON.

Dagster:
- Keep ops pure and idempotent (no side effects beyond defined outputs) unless explicitly for persistence.
- Use typed `Out` annotations / metadata where beneficial.
- Group related ops into jobs with clear naming (e.g. `metadata_extraction_job`).
- Validate datasets path via env var at op start; fail fast with descriptive error.

Streamlit Frontend:
- Maintain lightweight client layer in `api_client.py` (no direct requests in pages).
- Put expensive calculations behind `st.cache_data` / `st.cache_resource` as needed.

Logging:
- Reuse existing logging config; include context (request id) when possible.
- Do not introduce new root loggers; use `logging.getLogger(__name__)`.

Testing:
- Use Arrange/Act/Assert comments or blank line separation.
- Mock external calls; avoid hitting real Postgres for pure logic tests (use Django test DB fixtures).

Data Handling:
- Large files: stream read, don’t load entire dataset if not required.
- Add docstring referencing source dataset & columns when introducing new transformations.

## 8. Communication & Prompting Conventions
User Story Clarification Pattern:
"Goal: <business outcome>. Inputs: <files/data>. Constraints: <performance/security>. Output: <artifact>. Edge Cases: <list>."

Commit Message Template (Conventional Commit style):
<type>(scope): concise summary

Body:
- Motivation
- Implementation notes (bullets)
- Testing: how verified
- Follow-ups: list

Types: feat, fix, refactor, test, docs, chore, perf, ci.

Pull Request Description Template:
1. Summary
2. Motivation / Context
3. Changes (bulleted by file/component)
4. Screenshots / Logs (if UI or runtime changes)
5. Testing (commands + scenarios)
6. Risks & Mitigations
7. Follow-up Issues

Assistant Response Style:
- Begin with single-sentence purpose.
- Provide diff plan before applying broad edits.
- Use bullet lists; avoid fluffy filler.

## 9. Security & Secret Handling
- No plaintext secrets in code or commits; use env vars.
- Never echo secret values in agent output; redact as `***`.
- Assume statistical CSVs may contain pseudo‑PII—avoid exporting raw slices unless needed.
- Validate user-provided file paths against repository root (no path traversal).
- For third-party additions: pin versions (avoid `*` ranges). Check license compatibility (MIT/Apache preferred).

## 10. Agent Self‑Review Checklist (Run Before Final Output)
Code Changes:
- [ ] All touched files read entirely (critical sections) before modification.
- [ ] New functions typed; docstrings for non-trivial logic.
- [ ] No unrelated formatting churn.
- [ ] Migrations added if models changed.

Quality:
- [ ] Tests added/updated (happy + 1 edge case).
- [ ] All tests pass locally.
- [ ] Logging present for critical branches (not excessive).
- [ ] Error handling converts to consistent API error shapes.

Security & Safety:
- [ ] No secrets / credentials introduced.
- [ ] Inputs validated (sizes, types, nulls).
- [ ] External calls wrapped in timeouts / error handling.

Performance:
- [ ] Avoid O(n^2) over large datasets unless justified.
- [ ] Streaming or chunking used when reading large CSVs.

Docs & Communication:
- [ ] README / docs updated if public behavior changes.
- [ ] Commit message follows template.
- [ ] Follow-ups enumerated.

## 11. Prompt Templates (Copy/Paste)
System Prompt (General Coding Agent):
"You are a senior Python/Django/Dagster engineer. Follow AGENTS.md. Only modify necessary lines. Provide a diff plan first. Ensure tests and migrations are addressed."

User Prompt (Add DRF Endpoint):
"Add a paginated GET endpoint /api/v1/things/ to list Thing objects. Include serializer, URL route, basic test verifying pagination metadata. Return fields: id, name, created_at."

User Prompt (Add Dagster Op & Job):
"Create a Dagster op to compute skewness & kurtosis for numeric columns in application_train.csv, persist to statistics JSON, and wire into existing statistics job. Add tests with small synthetic dataframe."

User Prompt (Refactor):
"Refactor api.views.SomeView to extract business logic into a service function; ensure no behavior change and add unit tests for edge case: empty payload." 

User Prompt (Root-Cause Analysis Agent):
"Given latest metadata_*.json and statistics_*.json, detect top 3 anomalous columns (define anomaly) and hypothesize potential upstream data issues. Return YAML with keys: columns, reasons, recommended_actions."

Documentation Agent Prompt:
"Summarize new skewness feature for README: purpose, how to run, output location."

## 12. Structured Output Formats
When producing machine-consumable analysis, prefer:
```yaml
anomalies:
  - column: <name>
    issue: <short label>
    evidence: <metrics excerpt>
    hypothesis: <root cause>
    action: <recommended mitigation>
run_meta:
  generated_at: <iso8601>
  agent_version: <identifier>
```

## 13. Error Handling & Edge Cases Guidance
- Treat missing env vars: fail fast with actionable message.
- When adding parsers: handle empty files, malformed CSV rows, unexpected encodings.
- For statistical functions: guard division by zero / zero variance; return None or explicit sentinel with docstring.

## 14. Future Improvements (Backlog Suggestions)
- Add CI pipeline (lint, type-check, test) via GitHub Actions.
- Introduce ruff or flake8 + mypy for static analysis.
- Centralize service layer (avoid heavy views) under `backend/src/services/`.
- Add OpenAPI schema generation & endpoint docs (drf-spectacular).
- Implement caching for repeated statistics queries.
- Add data quality rules DSL & YAML-driven configuration.
- Create synthetic test dataset fixture to speed up stats tests.
- Add security headers & auth (API keys or JWT) for production.
- Container healthcheck endpoint in Django.

## 15. Conventions Summary (Quick Reference)
- Prefer minimal diffs; always show plan.
- Add tests first for non-trivial changes.
- Document new env vars & update this file.
- Use typed functions & explicit imports.
- Keep secrets out; review before commit.

---
This document is the single source of truth for AI agent collaboration. Update it whenever process or architecture meaningfully changes.
