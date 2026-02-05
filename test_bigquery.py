"""Test BigQuery connection and table access."""
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Setup credentials
credentials_path = r"C:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app\company_wide_cred.json"
project_id = "company-wide-370010"

credentials = service_account.Credentials.from_service_account_file(
    credentials_path,
    scopes=["https://www.googleapis.com/auth/bigquery"],
)
client = bigquery.Client(credentials=credentials, project=project_id)

print("=" * 60)
print("Testing BigQuery Table Access")
print("=" * 60)

# Test each table individually
tables_to_test = [
    "company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2",
    "company-wide-370010.Digibot.Daily_Rev_Metrics_by_Video_ID",
    "company-wide-370010.1_misc.YT_Transcript",
]

for table_name in tables_to_test:
    print(f"\nTesting: {table_name}")
    try:
        query = f"SELECT COUNT(*) as count FROM `{table_name}` LIMIT 1"
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            print(f"  [OK] Table accessible - Row count: {row.count}")

    except Exception as e:
        print(f"  [ERROR] {str(e)}")

print("\n" + "=" * 60)
print("Test Complete")
print("=" * 60)
