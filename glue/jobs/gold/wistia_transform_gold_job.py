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
# 1) Paths / tables
# ---------------------------------------------------------------------
silver_db = "wistia_silver"
silver_dim_table = "dim_media_metadata"
silver_fact_table = "fact_media_engagement"
gold_db = "wistia_gold"
gold_table = "media_metrics_summary"

silver_fact_path = "s3://rlk-wistia-video-analytics-dev/silver/fact_media_engagement/"
gold_path        = f"s3://rlk-wistia-video-analytics-dev/gold/{gold_table}/"

# ---------------------------------------------------------------------
# 2) Read Silver
# ---------------------------------------------------------------------
df_silver_fact = spark.read.parquet(silver_fact_path)

# DIM: select stable key + friendly title
df_dim_sel = (
    spark.table(f"{silver_db}.{silver_dim_table}")
         .select("media_id", F.col("title").alias("dim_title"))
         .dropDuplicates(["media_id"])   # one label per media_id
)

# ---------------------------------------------------------------------
# 3) Join & aggregate (one row per media_id per day)
# ---------------------------------------------------------------------
df_joined = (
    df_silver_fact.alias("f")
    .join(df_dim_sel.alias("d"), on="media_id", how="left")
)

df_gold = (
    df_joined
    .groupBy("media_id", F.col("f.load_date").alias("load_date"))
    .agg(
        F.first("dim_title", ignorenulls=True).alias("media_name"),
        F.sum("play_count").alias("total_plays"),
        F.sum("visitors").alias("total_visitors"),
        F.avg("play_rate").alias("avg_play_rate"),
        F.avg("engagement").alias("avg_engagement"),
    )
    .withColumn("media_name", F.coalesce(F.col("media_name"), F.col("media_id")))
)

# ---------------------------------------------------------------------
# 4) Write Gold (overwrite only today's partition)
# ---------------------------------------------------------------------
spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {gold_db}
    LOCATION 's3://rlk-wistia-video-analytics-dev/gold/'
""")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

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

