"""Check if specific video exists in BigQuery."""
from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    'company_wide_cred.json'
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

# Video ID from screenshot
video_id = 'ZJ0ZZLY03zU'  # "The Best Data Broker Removal Service..."

print(f"Checking for video: {video_id}\n")

# Check revenue table
query = """
SELECT
    video_id,
    Metrics_date,
    revenue,
    clicks,
    sales,
    organic_views
FROM `company-wide-370010.Digibot.Daily_Rev_Metrics_by_Video_ID`
WHERE video_id = @video_id
ORDER BY Metrics_date DESC
LIMIT 5
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
    ]
)

results = client.query(query, job_config=job_config).result()

print("Revenue Metrics Results:")
print("=" * 80)

found = False
for row in results:
    found = True
    print(f"Date: {row.Metrics_date}")
    print(f"Revenue: ${row.revenue}")
    print(f"Clicks: {row.clicks}")
    print(f"Sales: {row.sales}")
    print(f"Views: {row.organic_views}")
    print("-" * 80)

if not found:
    print("❌ No revenue data found for this video!")
    print("\nPossible reasons:")
    print("  1. Video ID is wrong (check the URL)")
    print("  2. Video hasn't generated any revenue yet")
    print("  3. Data hasn't been synced to BigQuery")
