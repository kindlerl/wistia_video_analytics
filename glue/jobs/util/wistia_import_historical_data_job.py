from pyspark.sql import SparkSession, functions as F

"""
Import archived data and append it to the existing partitioned Gold data
Step   Description
1️⃣ 	Readthe archived Gold Parquet data from S3
2️⃣ 	Add the correct load_date column for partitioning (if missing)
3️⃣ 	Append the data to the existing Goldtable
4️⃣ 	Validate via Athena
"""

spark = SparkSession.builder.appName("WistiaTransformGoldJob").enableHiveSupport().getOrCreate()

# ---------------------------------------------------------------------
# Define Paths and Table Names
# ---------------------------------------------------------------------
gold_archive_path = "s3://rlk-wistia-video-analytics-dev/gold_archive/media_metrics_summary_unpartitioned/"
gold_db = "wistia_gold"
gold_table = "media_metrics_summary"
gold_path   = f"s3://rlk-wistia-video-analytics-dev/gold/{gold_table}/"

# ---------------------------------------------------------------------
# 1️⃣ Read the archive data file
# ---------------------------------------------------------------------
df_old = spark.read.parquet(gold_archive_path)

# ---------------------------------------------------------------------
# 3️⃣ Since the archive data already has a load_date column, go ahead and append
# ---------------------------------------------------------------------
(
    df_old.write
    .mode("append")
    .format("parquet")
    .partitionBy("load_date")
    .option("path", gold_path)
    .saveAsTable(f"{gold_db}.{gold_table}")
)

print(f"✅ Archived Gold data imported successfully.")












