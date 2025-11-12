from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# ---------------------------------------------------------------------
# (1) Initialize Spark
# ---------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("WistiaTransformMetadataSilver")
    .enableHiveSupport()
    .getOrCreate()
)

# ---------------------------------------------------------------------
# (2) Define input/output paths and table info
# ---------------------------------------------------------------------
bronze_path = "s3://rlk-wistia-video-analytics-dev/bronze/media_metadata/"
silver_path = "s3://rlk-wistia-video-analytics-dev/silver/dim_media_metadata/"
silver_db   = "wistia_silver"
silver_table = "dim_media_metadata"

# ---------------------------------------------------------------------
# (3) Define Schema to match the filtered JSON from the Bronze job
#     From bronze job:
#       filtered_media = [
#           {
#               "id": m.get("id"),
#               "hashed_id": m.get("hashed_id"),
#               "name": m.get("name"),
#               "duration": m.get("duration"),
#               "created": m.get("created"),
#               "updated": m.get("updated"),
#               "thumbnail": m.get("thumbnail", {}).get("url"),
#                     "project": m.get("project", {}).get("name"),
#           }
#           for m in all_media
#       ]
# ---------------------------------------------------------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("hashed_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("created", StringType(), True),
    StructField("updated", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("project", StringType(), True)
])

# ---------------------------------------------------------------------
# (4) Read the Bronze JSON files
# ---------------------------------------------------------------------
raw_df = (
    spark.read
    .option("multiline", "true")
    .schema(schema)
    .json(f"{bronze_path}*.json")
)

# ---------------------------------------------------------------------
# (5) Transformations
# ---------------------------------------------------------------------
# After reading bronze/media_metadata/*.json into raw_df
df_dim = (
    raw_df.select(
        F.col("hashed_id").alias("media_id"),        # <- canonical key
        F.col("name").alias("title"),                # or keep 'name' and coalesce later
        F.to_timestamp("created").alias("created_at"),
        F.to_timestamp("updated").alias("updated_at"),
        'duration',
        'project',
        'thumbnail',
    )
    .withColumn('load_date', F.current_date())  # to be used as partition column
    .withColumn('ingestion_timestamp', F.current_timestamp())
)

# Log message
print('Schema aligned and timestamps standardized.')

# Dedupe
df_dim = df_dim.dropDuplicates(["media_id", "load_date"])

# ---------------------------------------------------------------------
# (6) Write to Silver Layer (Parquet & Glue Registration)
# ---------------------------------------------------------------------
(
    df_dim.write
    .mode('overwrite')
    .format('parquet')
    .option('path', silver_path)
    .partitionBy('load_date')
    .saveAsTable(f'{silver_db}.{silver_table}')      # Glue registration
)

# ---------------------------------------------------------------------
# (7) Done!
# ---------------------------------------------------------------------
print("Silver table 'dim_media_metadata' created successfully and registered in Glue Catalog.")

