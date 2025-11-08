from pyspark.sql import SparkSession, functions as F

"""
Step   Description
1️⃣ 	Read Silver Parquet data from S3
2️⃣ 	Group by load_date
3️⃣ 	Aggregate key metrics (sum, avg, count)
4️⃣ 	Write to Gold S3 path in Parquet format
5️⃣ 	Validate via Athena
"""

spark = SparkSession.builder.appName("WistiaTransformGoldJob").enableHiveSupport().getOrCreate()

# ---------------------------------------------------------------------
# 1️⃣ Define Paths and Table Names
# ---------------------------------------------------------------------
silver_db = "wistia_silver"
silver_table = "fact_media_engagement"
gold_db = "wistia_gold"
gold_table = "media_metrics_summary"

silver_path = f"s3://rlk-wistia-video-analytics-dev/silver/{silver_table}/"
gold_path   = f"s3://rlk-wistia-video-analytics-dev/gold/{gold_table}/"

# ---------------------------------------------------------------------
# 2️⃣ Read Silver Data
# ---------------------------------------------------------------------
df_silver = spark.read.parquet(silver_path)

# ---------------------------------------------------------------------
# 3️⃣ Aggregate Metrics by load_date
# ---------------------------------------------------------------------
df_gold = (
    df_silver.groupBy("load_date")
    .agg(
        F.sum("load_count").alias("total_loads"),
        F.sum("play_count").alias("total_plays"),
        F.avg("play_rate").alias("avg_play_rate"),
        F.sum("hours_watched").alias("total_hours_watched"),
        F.avg("engagement").alias("avg_engagement"),
        F.sum("visitors").alias("total_visitors"),
    )
    .withColumn("transformed_timestamp", F.current_timestamp())
)

# ---------------------------------------------------------------------
# 4️⃣ Write to Gold Layer (Overwrite per run)
# ---------------------------------------------------------------------
spark.sql("""
    CREATE DATABASE IF NOT EXISTS wistia_gold
    LOCATION 's3://rlk-wistia-video-analytics-dev/gold/'
""")

(
    df_gold.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .option("path", gold_path)
    .saveAsTable(f"{gold_db}.{gold_table}")
)

print(f"✅ Gold aggregation complete: {gold_db}.{gold_table}")
print(f"📁 Output path: {gold_path}")

