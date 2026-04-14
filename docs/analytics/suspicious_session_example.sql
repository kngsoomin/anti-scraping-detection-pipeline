SELECT
  event_time,
  path_template,
  status_code,
  referer
FROM sessionized_events
WHERE session_id = 'ecf94f7b1d68ee3d086dd62c6ad84ec56c869ac5080f476acfa129f7dc2105b7'
ORDER BY event_time
LIMIT 10;