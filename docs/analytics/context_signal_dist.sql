SELECT
  risk_band,
  COUNT(*) AS total_sessions,

  SUM(CASE WHEN context_tag = 'known_bot_candidate' THEN 1 ELSE 0 END) AS known_bot_candidate,
  ROUND(
    100.0 * SUM(CASE WHEN context_tag = 'known_bot_candidate' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS known_bot_pct,

  SUM(CASE WHEN context_tag = 'unresolved_host' THEN 1 ELSE 0 END) AS unresolved_host,
  ROUND(
    100.0 * SUM(CASE WHEN context_tag = 'unresolved_host' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS unresolved_host_pct,

  SUM(CASE WHEN context_tag = 'no_context_signal' THEN 1 ELSE 0 END) AS no_context_signal,
  ROUND(
    100.0 * SUM(CASE WHEN context_tag = 'no_context_signal' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS no_context_signal_pct

FROM suspicious_sessions
GROUP BY risk_band
ORDER BY
  CASE risk_band
    WHEN 'benign' THEN 1
    WHEN 'low' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'high' THEN 4
  END;