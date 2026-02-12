"""Fetch YouTube comments for ALL videos and store in local DB.

Usage:
    python scripts/fetch_all_comments.py

This fetches comments for all 2333+ videos from BigQuery,
detects pinned comments and affiliate brands, and stores in the local database.

YouTube API quota: ~2 requests per video = ~4,666 units (daily limit: 10,000)
Estimated time: ~60 minutes for 2,333 videos
"""
import os
import sys
import time
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv('.env.web')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    # Initialize services
    from app.services.local_db_service import LocalDBService
    from app.services.youtube_comments_service import YouTubeCommentsService
    from app.services.bigquery_service import BigQueryService

    logger.info("Initializing services...")

    local_db = LocalDBService()

    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
    project_id = os.getenv('BIGQUERY_PROJECT_ID', 'company-wide-370010')
    bq = BigQueryService(credentials_path=credentials_path, project_id=project_id, local_db=local_db)

    yt_cred_path = os.getenv('YOUTUBE_CREDENTIALS_PATH')
    yt = YouTubeCommentsService(credentials_path=yt_cred_path, local_db=local_db)

    if not yt.youtube:
        logger.error("YouTube API not initialized. Check YOUTUBE_CREDENTIALS_PATH.")
        return

    # Load known affiliate brands
    affiliates = bq.get_all_affiliates()
    if affiliates:
        yt.set_known_brands(affiliates)
        logger.info(f"Loaded {len(affiliates)} known affiliate brands")

    # Get all video IDs from BigQuery
    logger.info("Fetching all video IDs from BigQuery...")
    query = """
    SELECT video_id
    FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2`
    WHERE video_id IS NOT NULL
    ORDER BY video_id
    """
    query_job = bq.client.query(query)
    results = query_job.result()
    all_video_ids = [row.video_id for row in results]
    logger.info(f"Found {len(all_video_ids)} videos in BigQuery")

    # Check which videos already have comments in DB
    already_fetched = set()
    for vid in all_video_ids:
        if local_db.has_comments(vid):
            already_fetched.add(vid)

    remaining = [vid for vid in all_video_ids if vid not in already_fetched]
    logger.info(f"Already fetched: {len(already_fetched)}, Remaining: {len(remaining)}")

    if not remaining:
        logger.info("All videos already have comments fetched!")
        return

    # Fetch comments
    success = 0
    errors = 0
    skipped = 0
    start_time = time.time()

    for i, video_id in enumerate(remaining):
        try:
            count = yt.fetch_and_store(video_id, max_results=30)
            if count > 0:
                success += 1
            else:
                skipped += 1

            # Progress logging every 10 videos
            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                eta_seconds = (len(remaining) - i - 1) / rate if rate > 0 else 0
                eta_min = eta_seconds / 60
                logger.info(
                    f"Progress: {i+1}/{len(remaining)} | "
                    f"Success: {success}, Skipped: {skipped}, Errors: {errors} | "
                    f"Rate: {rate:.1f} vid/s | ETA: {eta_min:.0f} min"
                )

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        except Exception as e:
            errors += 1
            logger.error(f"Error for {video_id}: {e}")
            # Longer delay on error (might be rate limited)
            time.sleep(2)

    elapsed = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"DONE in {elapsed/60:.1f} minutes")
    logger.info(f"Success: {success}, Skipped: {skipped}, Errors: {errors}")
    logger.info(f"Total videos with comments: {len(already_fetched) + success}")


if __name__ == '__main__':
    main()
