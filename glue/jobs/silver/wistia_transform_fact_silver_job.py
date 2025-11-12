from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType, BooleanType
)

spark = (
    SparkSession.builder
    .appName("WistiaTransformFactSilver")
    .enableHiveSupport()
    .getOrCreate()
)

bronze_paths = [
    "s3://rlk-wistia-video-analytics-dev/bronze/gskhw4w4lm/",
    "s3://rlk-wistia-video-analytics-dev/bronze/v08dlrgr7v/"
]

# bronze_path = "s3://rlk-wistia-video-analytics-dev/bronze/"
silver_path = "s3://rlk-wistia-video-analytics-dev/silver/fact_media_engagement/"
silver_db   = "wistia_silver"
silver_tbl  = "fact_media_engagement"

schema = StructType([
    StructField("load_count",     IntegerType(), True),
    StructField("play_count",     IntegerType(), True),
    StructField("play_rate",      DoubleType(),  True),
    StructField("hours_watched",  DoubleType(),  True),
    StructField("engagement",     DoubleType(),  True),
    StructField("visitors",       IntegerType(), True),
])

# Read all stats files for both media IDs
df_fact = (
    spark.read
    .option("multiline", "true")
    .schema(schema)
    .json(bronze_paths)  # Only read from defined paths with known schemas
)

# Add hashed media_id and partition date extracted from filename
df_fact = (
    df_fact.withColumn(
        "media_id",
        F.regexp_extract(F.input_file_name(), r"bronze\/([^\/]+)\/", 1)
    )
    .withColumn(
        "load_date",
        F.to_date(F.regexp_extract(F.input_file_name(), r"stats_(\d{8})", 1), "yyyyMMdd")
    )
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

# Dedupe to avoid daily duplicates
df_fact = df_fact.dropDuplicates(["media_id", "load_date"])

# Write to Silver
(
    df_fact.write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("load_date")
      .option("path", silver_path)
      .option("mergeSchema", "true")
      .saveAsTable(f"{silver_db}.{silver_tbl}")
)

print("✅ fact_media_engagement rebuilt successfully.")

