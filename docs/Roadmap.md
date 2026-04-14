# Project Roadmap

## Phase 1
**Objective**
Build an end-to-end batch pipeline that transforms raw web logs into session-level behavioral features and explainable detection outputs.

**Completion Criteria**
- End-to-end execution for a given processing date (local / EC2)
  - environment-based config (env_name) controls execution
  
- Text → structured events via parsing (request parsing + path templating)

- Sessionization based on `(src_ip, user_agent)` with timeout-based session split
  - session boundary handled via inactivity threshold (includes lookback from previous day)
  
- Feature engineering (8 session-level features)
  - `requests_per_minute`
  - `session_duration_sec`
  - `unique_paths`
  - `unique_path_templates`
  - `missing_referer_ratio`
  - `html_to_asset_ratio`
  - `status_4xx_ratio`
  - `mean_inter_request_gap`
  
- Rule-based detection:
  - assign `rule_score`
  - map to `risk_band` (benign, low, medium, high)
  - explainability via `top_reasons`
  
- Session features stored as ML-ready snapshot

- Output datasets generated:
  - `session_features` (feature dataset)
  - `suspicious_sessions`, `daily_abuse_summary` (detection outputs)

- Outputs are stored with date partitioning to support replay/backfill

**Observed Limitations**
- Raw data (`access.log`) is fully scanned and parsed on each run → inefficient → requires partitioned ingestion
- Parsing is implemented using `rdd.map` → CPU-bound bottleneck
- Limited validation / data quality checks

## Phase 2
**Objective** 
Build a production-grade pipeline with data quality (DQ) metrics and validation framework

**Completion Criteria**
- Partition raw access logs and store under raw_logs/

- Add monitoring layer
  - metrics: collect row counts for each stage
  - manifest: capture runtime metadata for each run
    - run_id, process_date, duration, status

- validation framework (with severity separation: warning vs error)
  - row count consistency checks
  - dup checks
  - null checks
  - range checks
  - valid value checks

- execution model
  - support single-day execution
  - support date range backfill
  - parameterized process_date

- Add data quality observability
  - quarantine handling for malformed raw lines and parse failures
  - parse failure summary (conditional output)

**Observed Limitations**
- Fixed Spark timezone issue where event_time was incorrectly interpreted in session timezone (resolved)
- Detection is overly aggressive (high proportion of sessions flagged as suspicious)
- Behavior-only rules cannot distinguish benign crawlers (e.g. Googlebot) from malicious scrapers
- Lack of contextual enrichment (e.g. hostname/DNS) limits detection precision
- No UI or visualization layer for operational monitoring

## Phase 3
**Objective**  
Evolve the pipeline from a behavior-based detection system into a context-aware data product, enabling improved interpretation and downstream usability.

**Completion Criteria**

- Add context enrichment layer
  - integrate IP-to-hostname lookup dataset
  - derive context signals such as:
    - `has_hostname`
    - `is_known_bot_domain`
    - `is_known_bot_ua`

- Introduce `context_tag` for interpretation
  - combine hostname and user-agent signals
  - classify sessions into:
    - `known_bot_candidate`
    - `unresolved_host`
    - `no_context_signal`

- Maintain separation of concerns
  - ensure detection logic (`rule_score`) remains purely behavior-based
  - apply context signals only as annotations (not influencing detection)

- Extend data products
  - enrich `session_features` → `session_features_enriched` (ML-ready dataset)
  - propagate context signals to `suspicious_sessions` (analyst-facing dataset)

- Improve observability
  - track enrichment-level metrics (e.g., row consistency, context distribution)
  - validate that enrichment does not alter row-level granularity

- Enable query-based analysis
  - expose outputs via Athena for exploratory analysis
  - validate detection behavior using SQL-based investigation

- Strengthen testing
  - expand unit tests to cover enrichment and detection outputs

**Observed Limitations**

- Context signals remain incomplete and heuristic-based
  - hostname lookup coverage is limited
  - user-agent patterns can be spoofed

- Detection still relies on static rule thresholds
  - may not generalize well across different traffic distributions

- Lack of visualization layer
  - insights rely on manual querying rather than interactive dashboards