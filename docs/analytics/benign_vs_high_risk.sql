WITH base AS (
  SELECT
    CASE
      WHEN risk_band = 'high' THEN 'high_risk'
      WHEN risk_band = 'benign' THEN 'benign'
    END AS group_type,

    requests_per_minute,
    mean_inter_request_gap,
    unique_paths,
    missing_referer_ratio

  FROM suspicious_sessions
  WHERE risk_band IN ('benign', 'high')
),

agg AS (
  SELECT
    group_type,
    COUNT(*) AS session_cnt,
    AVG(requests_per_minute) AS avg_rpm,
    AVG(mean_inter_request_gap) AS avg_gap,
    AVG(unique_paths) AS avg_paths,
    AVG(missing_referer_ratio) AS avg_referer
  FROM base
  GROUP BY group_type
)

SELECT 'Requests per minute' AS feature,
       ROUND(MAX(CASE WHEN group_type = 'benign' THEN avg_rpm END), 2) AS benign,
       ROUND(MAX(CASE WHEN group_type = 'high_risk' THEN avg_rpm END), 2) AS high_risk
FROM agg

UNION ALL

SELECT 'Inter-request gap (sec)',
       ROUND(MAX(CASE WHEN group_type = 'benign' THEN avg_gap END), 2),
       ROUND(MAX(CASE WHEN group_type = 'high_risk' THEN avg_gap END), 2)
FROM agg

UNION ALL

SELECT 'Unique paths',
       ROUND(MAX(CASE WHEN group_type = 'benign' THEN avg_paths END), 2),
       ROUND(MAX(CASE WHEN group_type = 'high_risk' THEN avg_paths END), 2)
FROM agg

UNION ALL

SELECT 'Missing referer ratio',
       ROUND(MAX(CASE WHEN group_type = 'benign' THEN avg_referer END), 2),
       ROUND(MAX(CASE WHEN group_type = 'high_risk' THEN avg_referer END), 2)
FROM agg;