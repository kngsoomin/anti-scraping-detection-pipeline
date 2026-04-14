from src.common.config import load_config
from src.detection.build_rule_based_detection import build_rule_based_detection
from tests.conftest import SESSION_FEATURE_TEST_SCHEMA


def test_rule_based_detection_flags_high_risk_session(spark, session_feature_factory):
    config = load_config("local")
    thresholds = config.detection["thresholds"]
    risk_bands = config.detection["risk_bands"]

    rows = [
        session_feature_factory(
            session_id="s1",
            user_agent="BotUA",
            request_count=100,
            requests_per_minute=50.0,
            session_duration_sec=120,
            unique_paths=30,
            unique_path_templates=25,
            missing_referer_ratio=1.0,
            html_to_asset_ratio=5.0,
            status_4xx_ratio=0.4,
            mean_inter_request_gap=1.0,
            is_known_bot_ua=True,
        )
    ]

    df = spark.createDataFrame(rows, schema=SESSION_FEATURE_TEST_SCHEMA)
    out = build_rule_based_detection(
        session_features_df=df,
        thresholds=thresholds,
        risk_bands=risk_bands,
    )

    row = out.collect()[0]

    assert row["rule_score"] >= risk_bands["high_min_score"]
    assert row["risk_band"] == "high"
    assert "high_requests_per_minute" in row["top_reasons"]
    assert "high_unique_paths" in row["top_reasons"]
    assert "high_missing_referer_ratio" in row["top_reasons"]
    assert row["context_tag"] == "known_bot_candidate"


def test_rule_based_detection_marks_benign_session(spark, session_feature_factory):
    config = load_config("local")
    thresholds = config.detection["thresholds"]
    risk_bands = config.detection["risk_bands"]

    rows = [
        session_feature_factory(
            session_id="s2",
            src_ip="2.2.2.2",
            user_agent="Mozilla",
            request_count=5,
            requests_per_minute=1.0,
            session_duration_sec=300,
            unique_paths=4,
            unique_path_templates=3,
            missing_referer_ratio=0.1,
            html_to_asset_ratio=0.5,
            status_4xx_ratio=0.0,
            mean_inter_request_gap=30.0,
            has_hostname=False,
            is_known_bot_domain=False,
            is_known_bot_ua=False,
        )
    ]

    df = spark.createDataFrame(rows, schema=SESSION_FEATURE_TEST_SCHEMA)
    out = build_rule_based_detection(
        session_features_df=df,
        thresholds=thresholds,
        risk_bands=risk_bands,
    )

    row = out.collect()[0]

    assert row["rule_score"] == 0
    assert row["risk_band"] == "benign"
    assert row["top_reasons"] == []
    assert row["context_tag"] == "unresolved_host"