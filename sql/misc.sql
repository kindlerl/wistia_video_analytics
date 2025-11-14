SHOW TABLES IN wistia_gold;

SHOW PARTITIONS wistia_gold.media_metrics_summary;

MSCK REPAIR TABLE wistia_silver.fact_media_engagement;

SHOW TBLPROPERTIES wistia_silver.fact_media_engagement;

