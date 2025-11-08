## ⚙️ Step 1 — Create the Workflow File in Your GitHub Repo

In your repo (probably wistia-pipeline/), create the following path:

```bash
.github/workflows/deploy_wistia_pipeline.yml
```

Then paste this YAML inside:

```yaml
name: Deploy & Run Wistia ETL Workflow

on:
  workflow_dispatch:        # Allow manual trigger from GitHub UI
  schedule:
    - cron: "0 1 * * *"     # Optional: Run daily at 01:00 UTC (same as Glue schedule)

env:
  AWS_REGION: us-east-1
  GLUE_WORKFLOW_NAME: wistia_daily_workflow

jobs:
  deploy-and-run:
    name: Deploy & Trigger AWS Glue Workflow
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Validate AWS CLI connection
        run: aws sts get-caller-identity

      - name: Trigger Glue Workflow Run
        id: start_run
        run: |
          RUN_ID=$(aws glue start-workflow-run --name $GLUE_WORKFLOW_NAME --query 'RunId' --output text)
          echo "RUN_ID=$RUN_ID" >> $GITHUB_ENV
          echo "✅ Started workflow run ID: $RUN_ID"

      - name: Wait and monitor run status
        run: |
          STATUS=$(aws glue get-workflow-run --name $GLUE_WORKFLOW_NAME --run-id $RUN_ID --query 'Run.Status' --output text)
          echo "Initial Status: $STATUS"
          ATTEMPTS=0
          while [[ "$STATUS" == "RUNNING" && $ATTEMPTS -lt 60 ]]; do
            sleep 60
            STATUS=$(aws glue get-workflow-run --name $GLUE_WORKFLOW_NAME --run-id $RUN_ID --query 'Run.Status' --output text)
            echo "Current Status: $STATUS"
            ((ATTEMPTS++))
          done
          if [[ "$STATUS" != "COMPLETED" ]]; then
            echo "❌ Workflow failed with status: $STATUS"
            exit 1
          fi
          echo "🎉 Workflow completed successfully."

      - name: Notify completion
        if: success()
        run: echo "✅ Wistia ETL workflow completed successfully on $(date)"
```
<hr/>

## 🧰 Step 2 — Add AWS Credentials to GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions → New repository secret

Add:

| Name | Value |
| ---- | ----- |
| AWS_ACCESS_KEY_ID	| (From your IAM user)|
| AWS_SECRET_ACCESS_KEY	| (From your IAM user)|

Optional:

* You can also add AWS_REGION if you want to override it via secrets.
<hr/>

## 🧪 Step 3 — Test Manually

From your GitHub repo:

1. Go to Actions → Deploy & Run Wistia ETL Workflow
2. Click Run workflow → Run manually

Then check your AWS Glue → Workflows → wistia_daily_workflow  
You should see a new “Run” initiated with today’s timestamp

<hr/>

## 💡 Step 4 — Scheduling Notes

* The cron in this YAML (0 1 * * *) matches the Glue workflow schedule (01:00 UTC).
* If you want GitHub to only act as a manual trigger and not scheduled, just remove the schedule: block entirely.

✅ Summary of What This Adds
| Layer | Component | Purpose |
| --- | --- | ---|
| Infrastructure | AWS Glue Workflow | Automates Bronze → Silver → Gold execution |
| CI/CD | GitHub Actions | Enables pipeline deploy + execution with version control |
| Security | GitHub Secrets | Protects your AWS credentials |
| Reliability | Glue logs + GitHub logs | Full traceability for each run |