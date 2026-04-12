from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    description: str
    grain: str
    data_type: str
    formula: str
    null_handling: str
    interpretation: str
    caveats: Optional[str] = None


FEATURE_SPECS: dict[str, FeatureSpec] = {
    "requests_per_minute": FeatureSpec(
        name="requests_per_minute",
        description="Number of requests per minute within a session.",
        grain="session",
        data_type="double",
        formula="request_count / (session_duration_sec / 60), with fallback to request_count when session_duration_sec = 0",
        null_handling="Never null. If session_duration_sec is 0, use request_count as fallback.",
        interpretation="Higher values may indicate automated or scraper-like activity.",
        caveats="Very short sessions can inflate this metric even for benign traffic.",
    ),
    "session_duration_sec": FeatureSpec(
        name="session_duration_sec",
        description="Elapsed time in seconds between the first and last event in a session.",
        grain="session",
        data_type="bigint",
        formula="unix_timestamp(session_end_ts) - unix_timestamp(session_start_ts)",
        null_handling="Never null for valid sessions.",
        interpretation="Longer or extremely short sessions can both be informative depending on traffic pattern.",
        caveats="A single-event session will have duration 0.",
    ),
    "unique_paths": FeatureSpec(
        name="unique_paths",
        description="Count of distinct raw normalized paths requested within a session.",
        grain="session",
        data_type="bigint",
        formula="countDistinct(path)",
        null_handling="Never null.",
        interpretation="Higher values may indicate broad traversal across many endpoints.",
        caveats="Can be sensitive to high-cardinality paths if normalization is weak.",
    ),
    "unique_path_templates": FeatureSpec(
        name="unique_path_templates",
        description="Count of distinct path templates requested within a session.",
        grain="session",
        data_type="bigint",
        formula="countDistinct(path_template)",
        null_handling="Never null.",
        interpretation="Higher values may indicate broader navigation across endpoint families and are often more stable than raw path counts.",
        caveats="Heavily depends on path templating quality.",
    ),
    "missing_referer_ratio": FeatureSpec(
        name="missing_referer_ratio",
        description="Proportion of requests in a session with a missing referer.",
        grain="session",
        data_type="double",
        formula="avg(CASE WHEN referer IS NULL THEN 1 ELSE 0 END)",
        null_handling="Never null for non-empty sessions.",
        interpretation="Higher values may suggest direct scripted access without normal navigation context.",
        caveats="Some legitimate traffic also omits referer due to privacy settings, app browsers, or direct entry.",
    ),
    "html_to_asset_ratio": FeatureSpec(
        name="html_to_asset_ratio",
        description="Ratio of non-asset requests to asset requests within a session.",
        grain="session",
        data_type="double",
        formula="html_request_count / asset_request_count",
        null_handling="Null when asset_request_count = 0.",
        interpretation="Higher values may indicate scraper-like behavior because normal browsing often triggers many asset requests.",
        caveats="API-heavy or minimal front-end experiences may also produce high ratios.",
    ),
    "status_4xx_ratio": FeatureSpec(
        name="status_4xx_ratio",
        description="Proportion of session requests returning HTTP 4xx status codes.",
        grain="session",
        data_type="double",
        formula="avg(CASE WHEN status_code BETWEEN 400 AND 499 THEN 1 ELSE 0 END)",
        null_handling="Never null for non-empty sessions.",
        interpretation="Higher values may indicate probing, malformed requests, or unauthorized access attempts.",
        caveats="Can also rise due to broken client behavior or stale links.",
    ),
    "mean_inter_request_gap": FeatureSpec(
        name="mean_inter_request_gap",
        description="Average time gap in seconds between consecutive requests within a session.",
        grain="session",
        data_type="double",
        formula="avg(gap_from_prev_request_sec)",
        null_handling="Null for single-event sessions if no previous gap exists.",
        interpretation="Very small and highly regular gaps can indicate automation.",
        caveats="This metric alone does not capture timing regularity; standard deviation may be added later.",
    ),
}


FEATURE_COLUMNS = list(FEATURE_SPECS.keys())


def get_feature_spec(name: str) -> FeatureSpec:
    if name not in FEATURE_SPECS:
        raise KeyError(f"Unknown feature: {name}")
    return FEATURE_SPECS[name]