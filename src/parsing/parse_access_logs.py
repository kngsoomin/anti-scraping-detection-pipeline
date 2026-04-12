from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, parse_qsl

# Example log line
# 54.36.149.41 - - [22/Jan/2019:03:56:14 +0330]
# "GET /filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53 HTTP/1.1"
#   200
#   30577
#   "-"
#   "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)"
#   "-"
# 31.56.96.51 - - [22/Jan/2019:03:56:16 +0330]
#   "GET /image/60844/productModel/200x200 HTTP/1.1"
#   200
#   5667
#   "https://www.zanbil.ir/m/filter/b113"
#   "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36"
#   "-"
LOG_PATTERN = re.compile(
    r'^(?P<src_ip>\S+)\s+'                  # IP
    r'(?P<ident>\S+)\s+'                    # ident
    r'(?P<authuser>\S+)\s+'                 # authuser
    r'\[(?P<timestamp>[^\]]+)\]\s+'         # [22/Jan/2019:03:56:14 +0330]
    r'"(?P<request>[^"]*)"\s+'              # "GET /path HTTP/1.1"
    r'(?P<status_code>\d{3})\s+'            # 200
    r'(?P<bytes_sent>\S+)\s+'               # 30577 or -
    r'"(?P<referer>[^"]*)"\s+'              # "-"
    r'"(?P<user_agent>[^"]*)"'              # "Mozilla/5.0 ..."
    r'(?:\s+"(?P<extra>[^"]*)")?\s*$'       # optional final quoted field
)

REQUEST_PATTERN = re.compile(
    r'^(?P<method>[A-Z]+)\s+(?P<target>\S+)(?:\s+(?P<protocol>HTTP/\d.\d))?$'
)

DYNAMIC_SEGMENT_PATTERNS = [
    re.compile(r"^\d+$"),                   # numeric IDs
    re.compile(r"^[0-9a-fA-F]{8,}$"),       # hashes / long hex
]

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".ico"}
SCRIPT_EXTENSIONS = {".js"}
STYLE_EXTENSIONS = {".css"}
FONT_EXTENSIONS = {".woff", ".woff2", ".ttf", ".otf", ".eot"}

KNOWN_ASSET_PREFIXES = (
    "/image/",
    "/images/",
    "/img/",
    "/static/",
    "/assets/",
    "/fonts/",
)

KNOWN_API_PREFIXES = (
    "/api/",
)

ROBOTS_PATHS = {"/robots.txt", "/sitemap.xml"}

@dataclass
class ParseErrorRecord:
    raw_line: str
    error_type: str
    error_message: str
    ingested_at: str
    source_file: str
    line_hash: str


@dataclass
class NormalizedEvent:
    event_time: str
    event_date: str
    src_ip: str
    method: str
    raw_path: str
    path: str
    path_template: str
    query_string: Optional[str]
    protocol: Optional[str]
    status_code: int
    bytes_sent: Optional[int]
    referer: Optional[str]
    user_agent: Optional[str]
    is_asset: bool
    asset_type: str
    is_known_bot_ua: bool
    is_robots_request: bool
    ingested_at: str
    source_file: str
    line_hash: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_line(raw_line: str) -> str:
    return hashlib.sha256(raw_line.encode("utf-8")).hexdigest()


def normalize_dash(value: str) -> Optional[str]:
    return None if value == "-" else value


def parse_timestamp(raw_ts: str, ts_format: str = "%d/%b/%Y:%H:%M:%S %z") -> Optional[datetime]:
    # Example: 22/Jan/2019:03:56:14 +0330
    return datetime.strptime(raw_ts, ts_format)


def parse_request_line(request: str) -> dict[str, Optional[str]]:
    match = REQUEST_PATTERN.match(request.strip())
    if not match:
        raise ValueError(f"Invalid request line: {request}")

    return {
        "method": match.group("method"),
        "target": match.group("target"),
        "protocol": match.group("protocol"),
    }


def split_target(target: str) -> tuple[str, Optional[str]]:
    """
    Handles raw path like
    /product/10075/13903/%D9%85%D8%?model=39582
    """
    parsed = urlparse(target)
    path = parsed.path or "/"
    query = parsed.query or None
    return path, query


def _segment_to_template(segment: str) -> str:
    if not segment:
        return segment

    for pattern in DYNAMIC_SEGMENT_PATTERNS:
        if pattern.match(segment):
            return "{id}"

    return segment


def normalize_path_template(path: str) -> str:
    """
    Turn /product/123/456 into /product/{id}/{id}
    """
    parts = path.strip("/").split("/")
    if parts == [""]:
        return "/"

    templated = [_segment_to_template(part) for part in parts]
    generic_template = "/" + "/".join(templated)

    # endpoint-aware canonicalization
    if generic_template.startswith("/filter/"):
        return "/filter/{filter_expr}"
    if generic_template.startswith("/m/filter/"):
        return "/m/filter/{filter_expr}"

    if generic_template.startswith("/product/"):
        return "/product/{product_detail}"
    if generic_template.startswith("/m/product/"):
        return "/m/product/{product_detail}"

    return generic_template


def infer_asset_type(path: str) -> tuple[bool, str]:
    lower_path = path.lower()

    if lower_path in ROBOTS_PATHS:
        return False, "special"

    if any(lower_path.startswith(prefix) for prefix in KNOWN_API_PREFIXES):
        return False, "api"

    if any(lower_path.startswith(prefix) for prefix in KNOWN_ASSET_PREFIXES):
        if lower_path.startswith(("/image/", "/images/", "/img/")):
            return True, "image"
        return True, "asset"

    for ext in IMAGE_EXTENSIONS:
        if lower_path.endswith(ext):
            return True, "image"

    for ext in SCRIPT_EXTENSIONS:
        if lower_path.endswith(ext):
            return True, "js"

    for ext in STYLE_EXTENSIONS:
        if lower_path.endswith(ext):
            return True, "css"

    for ext in FONT_EXTENSIONS:
        if lower_path.endswith(ext):
            return True, "font"

    return False, "html"


def is_known_bot_user_agent(user_agent: Optional[str]) -> bool:
    if not user_agent:
        return False

    ua = user_agent.lower()
    bot_keywords = [
        "bot",
        "crawler",
        "spider",
        "ahrefsbot",
        "googlebot",
        "bingbot",
        "yandexbot",
        "duckduckbot",
        "semrushbot",
        "mj12bot",
    ]
    return any(keyword in ua for keyword in bot_keywords)


def parse_bytes_sent(raw_value: str) -> Optional[int]:
    if raw_value == "-":
        return None
    return int(raw_value)


def parse_raw_line(raw_line: str, source_file: str, ingested_at: Optional[str] = None) -> dict[str, Any]:
    """
    Parse one raw access-log line into either:
      {"ok": True, "event": {...}}
    or
      {"ok": False, "error": {...}}
    """
    ingested_at = ingested_at or utc_now_iso()
    line_hash = hash_line(raw_line)

    try:
        match = LOG_PATTERN.match(raw_line.strip())
        # print(match.groupdict())
        if not match:
            raise ValueError("Log line does not match expected combined-log pattern.")

        timestamp = parse_timestamp(match.group("timestamp"))
        request_info = parse_request_line(match.group("request"))
        path, query_string = split_target(request_info["target"] or "/")

        referer = normalize_dash(match.group("referer"))
        user_agent = normalize_dash(match.group("user_agent"))
        bytes_sent = parse_bytes_sent(match.group("bytes_sent"))
        is_asset, asset_type = infer_asset_type(path)

        event = NormalizedEvent(
            event_time=timestamp.astimezone(timezone.utc).isoformat(),
            event_date=timestamp.astimezone(timezone.utc).date().isoformat(),
            src_ip=match.group("src_ip"),
            method=request_info["method"] or "UNKNOWN",
            raw_path=request_info["target"] or "/",
            path=path,
            path_template=normalize_path_template(path),
            query_string=query_string,
            protocol=request_info["protocol"],
            status_code=int(match.group("status_code")),
            bytes_sent=bytes_sent,
            referer=referer,
            user_agent=user_agent,
            is_asset=is_asset,
            asset_type=asset_type,
            is_known_bot_ua=is_known_bot_user_agent(user_agent),
            is_robots_request=(path.lower() in ROBOTS_PATHS),
            ingested_at=ingested_at,
            source_file=source_file,
            line_hash=line_hash,
        )

        return {"ok": True, "event": asdict(event)}

    except Exception as exc:

        error = ParseErrorRecord(
            raw_line=raw_line.rstrip("\n"),
            error_type=type(exc).__name__,
            error_message=str(exc),
            ingested_at=ingested_at,
            source_file=source_file,
            line_hash=line_hash,
        )
        # raise exc
        return {"ok": False, "error": asdict(error)}