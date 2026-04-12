from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("validate-parsed-output").getOrCreate()

normalized_path = "data/output/normalized_events"
quarantined_path = "data/output/quarantined_raw_events"

normalized_df = spark.read.parquet(normalized_path)
error_df = spark.read.parquet(quarantined_path)

print("=== Row Counts ===")
print("normalized rows:", normalized_df.count())
print("error rows     :", error_df.count())

print("\n=== Partition Counts ===")
print("normalized partitions:", normalized_df.rdd.getNumPartitions())
print("error partitions     :", error_df.rdd.getNumPartitions())

print("\n=== Normalized Schema ===")
normalized_df.printSchema()

print("\n=== Error Schema ===")
error_df.printSchema()

print("\n=== Sample Normalized Rows ===")
normalized_df.show(10, truncate=False)

print("\n=== Sample Error Rows ===")
error_df.show(20, truncate=False)

print("\n=== Null Check (key columns) ===")
key_cols = ["event_time", "src_ip", "method", "path", "path_template", "status_code"]
for c in key_cols:
    null_count = normalized_df.filter(normalized_df[c].isNull()).count()
    print(f"{c}: {null_count}")

print("\n=== Distinct Asset Types ===")
normalized_df.select("asset_type").distinct().show(truncate=False)

print("\n=== Known Bot UA Count ===")
print(
    normalized_df.filter(normalized_df["is_known_bot_ua"] == True).count()
)

print("\n=== Error Type Distribution ===")
if "error_type" in error_df.columns:
    error_df.groupBy("error_type").count().orderBy("count", ascending=False).show(truncate=False)

spark.stop()