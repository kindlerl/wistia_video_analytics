UNLOAD (
  SELECT 
    media_id,
    media_name,
    CAST(total_plays AS VARCHAR) AS total_plays,
    CAST(total_visitors AS VARCHAR) AS total_visitors,
    CAST(avg_play_rate AS VARCHAR) AS avg_play_rate,
    CAST(avg_engagement AS VARCHAR) AS avg_engagement,
    CAST(load_date AS VARCHAR) AS load_date
  FROM wistia_gold.media_metrics_summary
)
TO 's3://rlk-wistia-video-analytics-dev/dashboard/sample_gold_data.csv'
WITH (format = 'CSV')

