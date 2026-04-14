from __future__ import annotations


def validate_run_metrics(metrics: dict[str, int]) -> tuple[bool, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    raw_partition_rows = metrics.get("raw_partition_rows", 0)
    normalized_rows = metrics.get("normalized_rows", 0)
    quarantined_raw_lines = metrics.get("quarantined_raw_lines", 0)
    parse_error_rows = metrics.get("parse_error_rows", 0)

    sessionized_rows = metrics.get("sessionized_rows", 0)
    session_feature_rows = metrics.get("session_feature_rows", 0)
    session_feature_enriched_rows = metrics.get("session_feature_enriched_rows", 0)

    suspicious_session_rows = metrics.get("suspicious_session_rows", 0)
    flagged_session_rows = metrics.get("flagged_session_rows", 0)

    summary_rows = metrics.get("summary_rows", 0)

    normalized_duplicate_rows = metrics.get("normalized_duplicate_rows", 0)
    session_feature_duplicate_rows = metrics.get("session_feature_duplicate_rows", 0)
    session_feature_enriched_duplicate_rows = metrics.get("session_feature_enriched_duplicate_rows", 0)
    suspicious_duplicate_rows = metrics.get("suspicious_duplicate_rows", 0)

    normalized_null_event_time_rows = metrics.get("normalized_null_event_time_rows", 0)
    normalized_null_src_ip_rows = metrics.get("normalized_null_src_ip_rows", 0)
    normalized_null_method_rows = metrics.get("normalized_null_method_rows", 0)
    normalized_null_path_rows = metrics.get("normalized_null_path_rows", 0)
    sessionized_null_session_id_rows = metrics.get("sessionized_null_session_id_rows", 0)

    invalid_missing_referer_ratio_rows = metrics.get("invalid_missing_referer_ratio_rows", 0)
    invalid_status_4xx_ratio_rows = metrics.get("invalid_status_4xx_ratio_rows", 0)
    invalid_html_to_asset_ratio_rows = metrics.get("invalid_html_to_asset_ratio_rows", 0)
    invalid_requests_per_minute_rows = metrics.get("invalid_requests_per_minute_rows", 0)
    invalid_session_duration_sec_rows = metrics.get("invalid_session_duration_sec_rows", 0)
    invalid_mean_inter_request_gap_rows = metrics.get("invalid_mean_inter_request_gap_rows", 0)

    invalid_risk_band_rows = metrics.get("invalid_risk_band_rows", 0)
    invalid_context_tag_rows = metrics.get("invalid_context_tag_rows", 0)

    benign_rows = metrics.get("benign_rows", 0)

    # row count sanity
    if normalized_rows > raw_partition_rows:
        errors.append("normalized_rows > raw_partition_rows")

    if quarantined_raw_lines > raw_partition_rows:
        errors.append("quarantined_raw_lines > raw_partition_rows")

    if session_feature_enriched_rows != session_feature_rows:
        errors.append("session_feature_enriched_rows != session_feature_rows")

    # grain sanity
    if session_feature_rows > sessionized_rows:
        errors.append("session_feature_rows > sessionized_rows")

    if suspicious_session_rows > session_feature_rows:
        errors.append("suspicious_session_rows > session_feature_rows")

    if suspicious_session_rows > session_feature_enriched_rows:
        errors.append("suspicious_session_rows > session_feature_enriched_rows")

    if flagged_session_rows > suspicious_session_rows:
        errors.append("flagged_session_rows > suspicious_session_rows")

    # output sanity
    if summary_rows not in (0, 1):
        errors.append("summary_rows must be 0 or 1")

    # duplicate checks
    if normalized_duplicate_rows > 0:
        warnings.append("normalized_duplicate_rows > 0")

    if session_feature_duplicate_rows > 0:
        errors.append("session_feature_duplicate_rows > 0")

    if session_feature_enriched_duplicate_rows > 0:
        errors.append("session_feature_enriched_duplicate_rows > 0")

    if suspicious_duplicate_rows > 0:
        errors.append("suspicious_duplicate_rows > 0")

    # parse/input quality warnings
    if quarantined_raw_lines > 0:
        warnings.append("quarantined_raw_lines > 0")

    if parse_error_rows > 0:
        warnings.append("parse_error_rows > 0")

    # null checks
    if normalized_null_event_time_rows > 0:
        errors.append("normalized_null_event_time_rows > 0")

    if normalized_null_src_ip_rows > 0:
        errors.append("normalized_null_src_ip_rows > 0")

    if normalized_null_method_rows > 0:
        errors.append("normalized_null_method_rows > 0")

    if normalized_null_path_rows > 0:
        errors.append("normalized_null_path_rows > 0")

    if sessionized_null_session_id_rows > 0:
        errors.append("sessionized_null_session_id_rows > 0")

    # range checks
    if invalid_missing_referer_ratio_rows > 0:
        errors.append("invalid_missing_referer_ratio_rows > 0")

    if invalid_status_4xx_ratio_rows > 0:
        errors.append("invalid_status_4xx_ratio_rows > 0")

    if invalid_html_to_asset_ratio_rows > 0:
        errors.append("invalid_html_to_asset_ratio_rows > 0")

    if invalid_requests_per_minute_rows > 0:
        errors.append("invalid_requests_per_minute_rows > 0")

    if invalid_session_duration_sec_rows > 0:
        errors.append("invalid_session_duration_sec_rows > 0")

    if invalid_mean_inter_request_gap_rows > 0:
        errors.append("invalid_mean_inter_request_gap_rows > 0")

    # allowed values
    if invalid_risk_band_rows > 0:
        errors.append("invalid_risk_band_rows > 0")

    if invalid_context_tag_rows > 0:
        errors.append("invalid_context_tag_rows > 0")

    # detector aggressiveness warnings
    if session_feature_rows > 0:
        flagged_ratio = flagged_session_rows / session_feature_rows
        if flagged_ratio > 0.9:
            warnings.append("flagged_ratio > 0.9")

    if session_feature_rows > 0 and benign_rows == 0:
        warnings.append("benign_rows == 0")

    dq_passed = len(errors) == 0
    return dq_passed, errors, warnings