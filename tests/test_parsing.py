from src.parsing.parse_access_logs import parse_raw_line


def test_parse_raw_line_success():
    line = (
        '54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] '
        '"GET /image/60844/productModel/200x200 HTTP/1.1" 200 5667 '
        '"https://www.zanbil.ir/m/filter/b113" '
        '"Mozilla/5.0 (Linux; Android 6.0)" "-"'
    )

    result = parse_raw_line(line, source_file="test.log")

    assert result["ok"] is True
    event = result["event"]

    assert event["src_ip"] == "54.36.149.41"
    assert event["method"] == "GET"
    assert event["path"] == "/image/60844/productModel/200x200"
    assert event["path_template"] == "/image/{id}/productModel/200x200"
    assert event["status_code"] == 200
    assert event["is_asset"] is True
    assert event["asset_type"] == "image"


def test_parse_raw_line_failure():
    bad_line = '149.202.169.246 - - [25/Jan/2019:05:47:45 +0330] "\\x15\\x03\\x01\\x00\\x02\\x02P" 400 166 "-" "-" "-"'

    result = parse_raw_line(bad_line, source_file="test.log")

    assert result["ok"] is False
    error = result["error"]

    assert error["error_type"] == "ValueError"
    assert "Invalid request line" in error["error_message"]