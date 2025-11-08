## Notes - Workflow wistia_daily_workflow

## The workflow automates daily execution:
1. Bronze Ingestion  
2. Silver Transformation (Fact + Metadata)  
3. Gold Aggregation

Execution is monitored via GitHub Actions (CI/CD) with Slack notifications.

---

## 🧾 Notes

- **Region:** `us-east-1`  
- **Storage Format:** Parquet (Silver/Gold)  
- **Catalog:** AWS Glue Data Catalog (Athena queryable)  
- **Incremental Ingestion:** Enabled by date-partitioned writes  

---

