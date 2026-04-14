WITH dist AS (
  SELECT
    suspicious_sessions_bucket,
    COUNT(*) AS ip_count
  FROM (
    SELECT
      src_ip,
      COUNT(*) AS suspicious_sessions,
      CASE
        WHEN COUNT(*) = 1 THEN '1'
        WHEN COUNT(*) BETWEEN 2 AND 5 THEN '2-5'
        WHEN COUNT(*) BETWEEN 6 AND 10 THEN '6-10'
        WHEN COUNT(*) BETWEEN 11 AND 20 THEN '11-20'
        ELSE '20+'
      END AS suspicious_sessions_bucket
    FROM suspicious_sessions
    WHERE risk_band IN ('medium', 'high')
    GROUP BY src_ip
  )
  GROUP BY suspicious_sessions_bucket
),

total AS (
  SELECT SUM(ip_count) AS total_ips FROM dist
)

SELECT
  d.suspicious_sessions_bucket,
  d.ip_count,
  ROUND(100.0 * d.ip_count / t.total_ips, 2) AS percentage
FROM dist d
CROSS JOIN total t
ORDER BY
  CASE d.suspicious_sessions_bucket
    WHEN '1' THEN 1
    WHEN '2-5' THEN 2
    WHEN '6-10' THEN 3
    WHEN '11-20' THEN 4
    WHEN '20+' THEN 5
  END;