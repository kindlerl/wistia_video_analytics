UNLOAD (
  SELECT 
    media_id,
    hashed_id,
    title,
    duration,
    project,
    thumbnail,
    CAST(created_at AS VARCHAR) AS created_at,
    CAST(updated_at AS VARCHAR) AS updated_at,
    CAST(ingestion_timestamp AS VARCHAR) AS ingestion_timestamp,
    CAST(load_date AS VARCHAR) AS load_date
  FROM wistia_silver.dim_media_metadata
)
TO 's3://rlk-wistia-video-analytics-dev/dashboard/sample_silver_data.csv'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');

UNLOAD (
  SELECT
    load_count,
    play_count,
    play_rate,
    hours_watched,
    engagement,
    visitors,
    media_id,
    CAST(ingestion_timestamp AS VARCHAR) AS ingestion_timestamp,
    CAST(load_date AS VARCHAR) AS load_date
  FROM wistia_silver.fact_media_engagement
)
TO 's3://rlk-wistia-video-analytics-dev/dashboard/sample_silver_fact_data.csv'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');
