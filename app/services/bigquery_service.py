"""BigQuery service for reading and writing YouTube analytics data."""
import logging
import os
import json
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from app.models import (
    Video, RevenueMetrics, VideoTranscript, ScriptAnalysis,
    AffiliateRecommendation, DescriptionAnalysis, ConversionAnalysis,
    AnalysisResults, DashboardStats, AffiliatePerformance
)

logger = logging.getLogger(__name__)


class BigQueryService:
    """Service for interacting with BigQuery tables."""

    def __init__(self, credentials_path: str, project_id: str, local_db=None):
        """
        Initialize BigQuery client with both BigQuery and Drive scopes.

        Args:
            credentials_path: Path to Google Cloud service account JSON file
            project_id: Google Cloud project ID
            local_db: LocalDBService instance for storing analysis results
        """
        self.project_id = project_id
        self.local_db = local_db  # For storing/reading analysis results

        # Try to load credentials from environment variable first (for cloud deployment)
        credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')

        if credentials_json:
            # Load from environment variable (Render, Cloud Run, etc.)
            logger.info("Loading credentials from GOOGLE_CREDENTIALS_JSON environment variable")
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/drive.readonly"
                ],
            )
        else:
            # Load from file (local development)
            logger.info(f"Loading credentials from file: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/drive.readonly"
                ],
            )

        self.client = bigquery.Client(
            credentials=credentials,
            project=project_id
        )
        logger.info(f"BigQuery client initialized for project: {project_id} with BigQuery + Drive scopes")

    # ============================================================================
    # READ OPERATIONS - Existing Tables
    # ============================================================================

    def get_videos(
        self,
        limit: int = 25,
        offset: int = 0,
        channel_code: Optional[str] = None,
        video_id: Optional[str] = None,
        has_analysis: Optional[bool] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Video]:
        """
        Fetch videos from BigQuery with optional filters.

        Args:
            limit: Number of videos to return
            offset: Number of videos to skip
            channel_code: Filter by channel code
            video_id: Filter by video ID (partial match)
            has_analysis: Filter by whether video has analysis
            start_date: Filter videos published after this date
            end_date: Filter videos published before this date

        Returns:
            List of Video objects
        """
        # Query with LEFT JOIN to prioritize videos that have transcripts
        query = """
        SELECT
            v.video_id,
            v.Channel_Code as channel_code,
            v.Video_Title as title,
            v.Video_published_date as published_date,
            v.Video_URL as video_url,
            v.Video_description as description,
            CASE WHEN t.Video_ID IS NOT NULL THEN 1 ELSE 0 END as has_transcript
        FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2` v
        LEFT JOIN `company-wide-370010.1_misc.YT_Transcript` t
          ON v.video_id = t.Video_ID
        WHERE 1=1
        """

        params = []

        if channel_code:
            # Case-insensitive partial match for channel code
            query += " AND UPPER(v.Channel_Code) LIKE @channel_code"
            params.append(bigquery.ScalarQueryParameter("channel_code", "STRING", f"%{channel_code.upper()}%"))

        if video_id:
            # Case-insensitive partial match for video ID or title
            query += " AND (UPPER(v.video_id) LIKE @video_id OR UPPER(v.Video_Title) LIKE @video_id)"
            params.append(bigquery.ScalarQueryParameter("video_id", "STRING", f"%{video_id.upper()}%"))

        if start_date:
            query += " AND Video_published_date >= @start_date"
            params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))

        if end_date:
            query += " AND Video_published_date <= @end_date"
            params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

        # If filtering by analysis status, fetch more videos to account for filtering
        # This ensures we get enough results after filtering
        fetch_limit = limit * 3 if has_analysis is not None else limit

        query += " ORDER BY has_transcript DESC, v.Video_published_date DESC LIMIT @limit OFFSET @offset"
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", fetch_limit))
        params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()

        videos = []
        for row in results:
            # Construct YouTube URL if not present in database
            video_url = row.video_url if row.video_url else f"https://www.youtube.com/watch?v={row.video_id}"

            # Check if video has analysis in local database
            video_has_analysis = False
            latest_analysis_date = None
            if self.local_db:
                script_analysis = self.local_db.get_script_analysis(row.video_id)
                if script_analysis:
                    video_has_analysis = True
                    latest_analysis_date = script_analysis.analysis_timestamp

            # Apply has_analysis filter if specified
            if has_analysis is not None:
                if has_analysis and not video_has_analysis:
                    continue  # Skip videos without analysis when filtering for analyzed only
                if not has_analysis and video_has_analysis:
                    continue  # Skip analyzed videos when filtering for not analyzed

            videos.append(Video(
                video_id=row.video_id,
                channel_code=row.channel_code,
                title=row.title,
                published_date=row.published_date,
                video_url=video_url,
                description=row.description,
                has_analysis=video_has_analysis,
                latest_analysis_date=latest_analysis_date
            ))

        logger.info(f"Fetched {len(videos)} videos from BigQuery (filtered to {len(videos)} after analysis status check)")
        return videos

    def get_video_by_id(self, video_id: str) -> Optional[Video]:
        """
        Fetch a single video by ID.

        Args:
            video_id: YouTube video ID

        Returns:
            Video object or None if not found
        """
        query = """
        SELECT
            video_id,
            Channel_Code as channel_code,
            Video_Title as title,
            Video_published_date as published_date,
            Video_URL as video_url,
            Video_description as description
        FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2`
        WHERE video_id = @video_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()

        for row in results:
            # Construct YouTube URL if not present in database
            video_url = row.video_url if row.video_url else f"https://www.youtube.com/watch?v={row.video_id}"

            # Get description - try main table first, fallback to SERP table
            description = row.description
            if not description or (isinstance(description, str) and not description.strip()):
                description = self._get_description_from_serp(video_id)

            return Video(
                video_id=row.video_id,
                channel_code=row.channel_code,
                title=row.title,
                published_date=row.published_date,
                video_url=video_url,
                description=description,
                has_analysis=False,
                latest_analysis_date=None
            )

        logger.warning(f"Video not found: {video_id}")
        return None

    def _get_description_from_serp(self, video_id: str) -> Optional[str]:
        """
        Fetch video description from SERP table as fallback.

        Args:
            video_id: YouTube video ID

        Returns:
            Description text or None if not found
        """
        try:
            query = """
            SELECT Description
            FROM `company-wide-370010.1_YT_Serp_result.ALL_Time YT Serp`
            WHERE video_id = @video_id
              AND Description IS NOT NULL
              AND TRIM(Description) != ''
              AND published_date IS NOT NULL
              AND Scrape_date IS NOT NULL
            ORDER BY Scrape_date DESC
            LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            for row in results:
                if row.Description:
                    logger.info(f"Found description from SERP table for video: {video_id}")
                    return row.Description

            logger.debug(f"No description found in SERP table for video: {video_id}")
            return None

        except Exception as e:
            logger.warning(f"Error fetching description from SERP table: {str(e)}")
            return None

    def get_transcript(self, video_id: str) -> Optional[str]:
        """
        Fetch video transcript from BigQuery.

        Args:
            video_id: YouTube video ID

        Returns:
            Transcript text or None if not found
        """
        query = """
        SELECT transcript
        FROM `company-wide-370010.1_misc.YT_Transcript`
        WHERE video_id = @video_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()

        for row in results:
            return row.transcript

        logger.warning(f"Transcript not found for video: {video_id}")
        return None

    def get_yt_analytics_by_source(self, video_id: str, days: int = 90) -> List[Dict]:
        """
        Fetch YouTube Analytics data by traffic source for the last N days.

        Args:
            video_id: YouTube video ID
            days: Number of days to look back (default 90)

        Returns:
            List of dictionaries with traffic source metrics
        """
        query = """
        SELECT
            yt.Date,
            yt.channel,
            yt.Traffic_source,
            yt.views,
            yt.impression,
            yt.impression_CTR,
            yt.average_view_percentage,
            gi.video_title,
            gi.main_keyword,
            gi.silo
        FROM `company-wide-370010.Digibot.Digibot_YT_analytics` yt
        JOIN `company-wide-370010.Digibot.Digibot_General_info` gi
            ON yt.Video_ID = gi.video_id
        WHERE yt.Video_ID = @video_id
          AND yt.Date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY yt.Date DESC, yt.Traffic_source
        """

        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("video_id", "STRING", video_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            analytics_data = []
            for row in results:
                analytics_data.append({
                    'date': row.Date.isoformat() if row.Date else None,
                    'channel': row.channel,
                    'traffic_source': row.Traffic_source,
                    'views': int(row.views) if row.views else 0,
                    'impressions': int(row.impression) if row.impression else 0,
                    'impression_ctr': float(row.impression_CTR) if row.impression_CTR else 0.0,
                    'avg_view_percentage': float(row.average_view_percentage) if row.average_view_percentage else 0.0,
                    'video_title': row.video_title,
                    'main_keyword': row.main_keyword,
                    'silo': row.silo
                })

            logger.info(f"Fetched {len(analytics_data)} YT Analytics records for video: {video_id}")
            return analytics_data

        except Exception as e:
            logger.error(f"Error fetching YT Analytics data: {str(e)}")
            return []

    def get_yt_analytics_summary(self, video_id: str, days: int = 90) -> Dict:
        """
        Get aggregated YouTube Analytics summary by traffic source.

        Args:
            video_id: YouTube video ID
            days: Number of days to look back (default 90)

        Returns:
            Dictionary with summary metrics by traffic source
        """
        query = """
        WITH source_metrics AS (
            SELECT
                yt.Traffic_source,
                SUM(yt.views) as total_views,
                SUM(yt.impression) as total_impressions,
                AVG(yt.impression_CTR) as avg_ctr,
                AVG(yt.average_view_percentage) as avg_view_pct,
                COUNT(*) as data_points
            FROM `company-wide-370010.Digibot.Digibot_YT_analytics` yt
            WHERE yt.Video_ID = @video_id
              AND yt.Date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
            GROUP BY yt.Traffic_source
        ),
        video_info AS (
            SELECT
                gi.video_title,
                gi.main_keyword,
                gi.silo,
                gi.presenter as channel
            FROM `company-wide-370010.Digibot.Digibot_General_info` gi
            WHERE gi.video_id = @video_id
            LIMIT 1
        )
        SELECT
            sm.Traffic_source,
            sm.total_views,
            sm.total_impressions,
            ROUND(sm.avg_ctr, 2) as avg_ctr,
            ROUND(sm.avg_view_pct, 2) as avg_view_pct,
            sm.data_points,
            vi.video_title,
            vi.main_keyword,
            vi.silo,
            vi.channel
        FROM source_metrics sm
        CROSS JOIN video_info vi
        ORDER BY sm.total_views DESC
        """

        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("video_id", "STRING", video_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            by_source = []
            video_info = {}
            total_views = 0
            total_impressions = 0

            for row in results:
                by_source.append({
                    'traffic_source': row.Traffic_source,
                    'views': int(row.total_views) if row.total_views else 0,
                    'impressions': int(row.total_impressions) if row.total_impressions else 0,
                    'avg_ctr': float(row.avg_ctr) if row.avg_ctr else 0.0,
                    'avg_view_percentage': float(row.avg_view_pct) if row.avg_view_pct else 0.0,
                    'data_points': int(row.data_points) if row.data_points else 0
                })
                total_views += int(row.total_views) if row.total_views else 0
                total_impressions += int(row.total_impressions) if row.total_impressions else 0

                # Get video info from first row
                if not video_info:
                    video_info = {
                        'video_title': row.video_title,
                        'main_keyword': row.main_keyword,
                        'silo': row.silo,
                        'channel': row.channel
                    }

            # Calculate overall CTR
            overall_ctr = (total_views / total_impressions * 100) if total_impressions > 0 else 0.0

            summary = {
                'video_id': video_id,
                'days_analyzed': days,
                'total_views': total_views,
                'total_impressions': total_impressions,
                'overall_ctr': round(overall_ctr, 2),
                'by_traffic_source': by_source,
                **video_info
            }

            logger.info(f"Generated YT Analytics summary for video: {video_id} ({len(by_source)} traffic sources)")
            return summary

        except Exception as e:
            logger.error(f"Error getting YT Analytics summary: {str(e)}")
            return {
                'video_id': video_id,
                'days_analyzed': days,
                'total_views': 0,
                'total_impressions': 0,
                'overall_ctr': 0.0,
                'by_traffic_source': [],
                'error': str(e)
            }

    def get_revenue_metrics(self, video_id: str) -> Optional[RevenueMetrics]:
        """
        Fetch aggregated revenue metrics for a video across all months.
        Includes CTR data from YT Analytics table.

        Args:
            video_id: YouTube video ID

        Returns:
            RevenueMetrics object with totals or None if not found
        """
        query = """
        WITH monthly_metrics AS (
            SELECT
                m.video_id,
                g.presenter as channel,
                g.video_title,
                g.main_keyword,
                m.metrics_month_year,
                m.revenue,
                m.clicks,
                m.sales,
                m.organic_views
            FROM `company-wide-370010.Digibot.Metrics_by_Month` m
            LEFT JOIN `company-wide-370010.Digibot.Digibot_General_info` g
              ON m.video_id = g.video_id
            WHERE m.video_id = @video_id
        ),
        yt_analytics AS (
            SELECT
                y.Video_ID,
                AVG(y.impression_CTR) as avg_impression_ctr
            FROM `company-wide-370010.Digibot.Digibot_YT_analytics` y
            WHERE y.Video_ID = @video_id
            GROUP BY y.Video_ID
        ),
        totals AS (
            SELECT
                mm.video_id,
                MAX(mm.channel) as channel,
                MAX(mm.metrics_month_year) as latest_month,
                SUM(mm.revenue) as total_revenue,
                SUM(mm.clicks) as total_clicks,
                SUM(mm.sales) as total_sales,
                SUM(mm.organic_views) as total_views,
                SAFE_DIVIDE(SUM(mm.sales), SUM(mm.organic_views)) * 100 as conversion_rate,
                SAFE_DIVIDE(SUM(mm.revenue), SUM(mm.clicks)) as revenue_per_click
            FROM monthly_metrics mm
            GROUP BY mm.video_id
        )
        SELECT
            t.video_id,
            t.channel,
            t.latest_month,
            t.total_revenue,
            t.total_clicks,
            t.total_sales,
            t.total_views,
            ROUND(t.conversion_rate, 3) as conversion_rate,
            ROUND(t.revenue_per_click, 2) as revenue_per_click,
            ROUND(y.avg_impression_ctr, 2) as avg_impression_ctr
        FROM totals t
        LEFT JOIN yt_analytics y ON t.video_id = y.Video_ID
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()

        for row in results:
            revenue = float(row.total_revenue) if row.total_revenue else 0.0
            clicks = int(row.total_clicks) if row.total_clicks else 0
            sales = int(row.total_sales) if row.total_sales else 0
            views = int(row.total_views) if row.total_views else 0

            # Use BigQuery calculated metrics (more accurate)
            conversion_rate = float(row.conversion_rate) if row.conversion_rate else 0.0
            revenue_per_click = float(row.revenue_per_click) if row.revenue_per_click else 0.0
            revenue_per_1k_views = (revenue / views * 1000) if views > 0 else 0.0

            # Get CTR from YT Analytics (already in percentage format)
            impression_ctr = float(row.avg_impression_ctr) if row.avg_impression_ctr else 0.0

            return RevenueMetrics(
                video_id=row.video_id,
                channel=row.channel or "Unknown",
                metrics_date=row.latest_month,
                revenue=revenue,
                clicks=clicks,
                sales=sales,
                organic_views=views,
                conversion_rate=conversion_rate,
                revenue_per_click=revenue_per_click,
                revenue_per_1k_views=revenue_per_1k_views,
                impression_ctr=impression_ctr
            )

        logger.warning(f"Revenue metrics not found for video: {video_id}")
        return None

    def get_affiliate_performance(self, video_id: str) -> list:
        """
        Fetch per-tracking-ID affiliate performance for a video.

        Args:
            video_id: YouTube video ID

        Returns:
            List of AffiliatePerformance objects sorted by revenue DESC
        """
        query = """
        SELECT
            video_id,
            Tracking_Id,
            Platform,
            Affiliate,
            Link_Placement,
            SUM(revenue) as total_revenue,
            SUM(clicks) as total_clicks,
            SUM(sales) as total_sales,
            SAFE_DIVIDE(SUM(sales), SUM(clicks)) * 100 as conversion_rate,
            SAFE_DIVIDE(SUM(revenue), SUM(clicks)) as revenue_per_click
        FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
        WHERE video_id = @video_id
        GROUP BY video_id, Tracking_Id, Platform, Affiliate, Link_Placement
        ORDER BY total_revenue DESC
        """

        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            performance_data = []
            for row in results:
                performance_data.append(AffiliatePerformance(
                    video_id=row.video_id,
                    tracking_id=row.Tracking_Id or "Unknown",
                    platform=row.Platform or "Unknown",
                    affiliate=row.Affiliate or "Unknown",
                    link_placement=row.Link_Placement or "Unknown",
                    total_revenue=float(row.total_revenue) if row.total_revenue else 0.0,
                    total_clicks=int(row.total_clicks) if row.total_clicks else 0,
                    total_sales=int(row.total_sales) if row.total_sales else 0,
                    conversion_rate=float(row.conversion_rate) if row.conversion_rate else 0.0,
                    revenue_per_click=float(row.revenue_per_click) if row.revenue_per_click else 0.0,
                ))

            logger.info(f"Fetched {len(performance_data)} affiliate tracking IDs for video: {video_id}")
            return performance_data

        except Exception as e:
            logger.error(f"Error fetching affiliate performance: {str(e)}")
            return []

    # ============================================================================
    # WRITE OPERATIONS - Analysis Results to Existing Tables
    # ============================================================================

    def store_script_analysis(self, analysis: ScriptAnalysis) -> bool:
        """
        Store script analysis results to local database (not BigQuery).

        Args:
            analysis: ScriptAnalysis object

        Returns:
            True if successful, False otherwise
        """
        if self.local_db:
            return self.local_db.store_script_analysis(analysis)
        else:
            logger.warning("No local database configured, cannot store script analysis")
            return False

    def store_affiliate_recommendations(
        self,
        recommendations: List[AffiliateRecommendation]
    ) -> bool:
        """
        Store affiliate recommendations to local database (not BigQuery).

        Args:
            recommendations: List of AffiliateRecommendation objects

        Returns:
            True if successful, False otherwise
        """
        if self.local_db:
            return self.local_db.store_affiliate_recommendations(recommendations)
        else:
            logger.warning("No local database configured, cannot store affiliate recommendations")
            return False

    def store_description_analysis(self, analysis: DescriptionAnalysis) -> bool:
        """
        Store description analysis to local database (not BigQuery).

        Args:
            analysis: DescriptionAnalysis object

        Returns:
            True if successful, False otherwise
        """
        if self.local_db:
            return self.local_db.store_description_analysis(analysis)
        else:
            logger.warning("No local database configured, cannot store description analysis")
            return False

    def store_conversion_analysis(self, analysis: ConversionAnalysis) -> bool:
        """
        Store conversion analysis to local database (not BigQuery).

        Args:
            analysis: ConversionAnalysis object

        Returns:
            True if successful, False otherwise
        """
        if self.local_db:
            return self.local_db.store_conversion_analysis(analysis)
        else:
            logger.warning("No local database configured, cannot store conversion analysis")
            return False

    # ============================================================================
    # READ OPERATIONS - Analysis Results
    # ============================================================================

    def get_latest_analysis(self, video_id: str) -> Optional[AnalysisResults]:
        """
        Fetch the latest complete analysis results for a video.

        Args:
            video_id: YouTube video ID

        Returns:
            AnalysisResults object or None if not found
        """
        # Get video info
        video = self.get_video_by_id(video_id)
        if not video:
            return None

        # Get revenue metrics
        revenue_metrics = self.get_revenue_metrics(video_id)

        # Get script analysis
        script_analysis = self._get_script_analysis(video_id)

        # Get affiliate recommendations
        affiliate_recs = self._get_affiliate_recommendations(video_id)

        # Get description analysis
        description_analysis = self._get_description_analysis(video_id)

        # Get conversion analysis
        conversion_analysis = self._get_conversion_analysis(video_id)

        # Get real affiliate performance from BigQuery (no analysis needed)
        affiliate_performance = self.get_affiliate_performance(video_id)

        # Compute existing links analysis from description (regex only, no AI)
        # Pass known affiliate names from BigQuery so detection isn't hardcoded
        existing_links_analysis = None
        if video.description:
            try:
                from app.services.affiliate_recommender import AffiliateRecommender
                known_affiliates = list({p.affiliate for p in affiliate_performance}) if affiliate_performance else None
                existing_links_analysis = AffiliateRecommender.analyze_existing_links(
                    video.description, known_affiliates=known_affiliates
                )
            except Exception as e:
                logger.error(f"Error analyzing existing links: {e}")

        return AnalysisResults(
            video=video,
            revenue_metrics=revenue_metrics,
            script_analysis=script_analysis,
            affiliate_recommendations=affiliate_recs,
            description_analysis=description_analysis,
            conversion_analysis=conversion_analysis,
            affiliate_performance=affiliate_performance,
            existing_links_analysis=existing_links_analysis
        )

    def _get_script_analysis(self, video_id: str) -> Optional[ScriptAnalysis]:
        """Fetch latest script analysis for a video from local database."""
        if self.local_db:
            return self.local_db.get_script_analysis(video_id)
        else:
            logger.warning("No local database configured, cannot fetch script analysis")
            return None

    def _get_affiliate_recommendations(self, video_id: str) -> List[AffiliateRecommendation]:
        """Fetch latest affiliate recommendations for a video from local database."""
        if self.local_db:
            return self.local_db.get_affiliate_recommendations(video_id)
        else:
            logger.warning("No local database configured, cannot fetch affiliate recommendations")
            return []

    def _get_description_analysis(self, video_id: str) -> Optional[DescriptionAnalysis]:
        """Fetch latest description analysis for a video from local database."""
        if self.local_db:
            return self.local_db.get_description_analysis(video_id)
        else:
            logger.warning("No local database configured, cannot fetch description analysis")
            return None

    def _get_conversion_analysis(self, video_id: str) -> Optional[ConversionAnalysis]:
        """Fetch latest conversion analysis for a video from local database."""
        if self.local_db:
            return self.local_db.get_conversion_analysis(video_id)
        else:
            logger.warning("No local database configured, cannot fetch conversion analysis")
            return None

    # ============================================================================
    # DASHBOARD STATISTICS
    # ============================================================================

    def _get_local_analysis_stats(self) -> Dict:
        """Get analysis statistics from local database."""
        if not self.local_db:
            return {
                'analyzed_videos': 0,
                'avg_script_quality': 0.0,
                'avg_hook_score': 0.0,
                'avg_cta_score': 0.0,
                'avg_conversion_rate': 0.0
            }

        try:
            # Use the local_db service's connection method (supports both SQLite and PostgreSQL)
            conn = self.local_db._get_connection()
            cursor = conn.cursor()

            # Get script analysis stats
            cursor.execute("""
            SELECT
                COUNT(DISTINCT video_id) as analyzed_videos,
                AVG(script_quality_score) as avg_script_quality,
                AVG(hook_effectiveness_score) as avg_hook_score,
                AVG(call_to_action_score) as avg_cta_score
            FROM script_analysis
            """)
            script_row = cursor.fetchone()

            # Get avg conversion rate from conversion_analysis table
            cursor.execute("""
            SELECT AVG(conversion_rate) as avg_conversion_rate
            FROM conversion_analysis
            WHERE conversion_rate IS NOT NULL AND conversion_rate > 0
            """)
            conv_row = cursor.fetchone()
            conn.close()

            avg_conversion_rate = 0.0
            if conv_row and conv_row[0]:
                avg_conversion_rate = round(conv_row[0], 2)

            if script_row and script_row[0]:
                return {
                    'analyzed_videos': script_row[0] or 0,
                    'avg_script_quality': round(script_row[1], 1) if script_row[1] else 0.0,
                    'avg_hook_score': round(script_row[2], 1) if script_row[2] else 0.0,
                    'avg_cta_score': round(script_row[3], 1) if script_row[3] else 0.0,
                    'avg_conversion_rate': avg_conversion_rate
                }
        except Exception as e:
            logger.error(f"Error getting local analysis stats: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return {
            'analyzed_videos': 0,
            'avg_script_quality': 0.0,
            'avg_hook_score': 0.0,
            'avg_cta_score': 0.0,
            'avg_conversion_rate': 0.0
        }

    def get_dashboard_stats(self) -> DashboardStats:
        """
        Calculate dashboard statistics (simplified for tables that exist).

        Returns:
            DashboardStats object
        """
        try:
            # Query using correct revenue table (Metrics_by_Month)
            query = """
            WITH video_stats AS (
                SELECT COUNT(DISTINCT video_id) as total_videos
                FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2`
            ),
            revenue_stats AS (
                SELECT
                    SUM(revenue) as total_revenue,
                    SUM(organic_views) as total_views,
                    COUNT(DISTINCT video_id) as videos_with_revenue
                FROM `company-wide-370010.Digibot.Metrics_by_Month`
            )
            SELECT
                v.total_videos,
                IFNULL(r.total_revenue, 0.0) as total_revenue,
                IFNULL(r.total_views, 0) as total_views,
                CASE
                    WHEN r.videos_with_revenue > 0
                    THEN IFNULL(r.total_revenue / r.videos_with_revenue, 0.0)
                    ELSE 0.0
                END as avg_revenue_per_video
            FROM video_stats v
            CROSS JOIN revenue_stats r
            """

            query_job = self.client.query(query)
            results = query_job.result()

            for row in results:
                # Get analysis stats from local database
                analysis_stats = self._get_local_analysis_stats()

                return DashboardStats(
                    total_videos=row.total_videos,
                    analyzed_videos=analysis_stats['analyzed_videos'],
                    avg_script_quality=analysis_stats['avg_script_quality'],
                    avg_hook_score=analysis_stats['avg_hook_score'],
                    avg_cta_score=analysis_stats['avg_cta_score'],
                    avg_conversion_rate=analysis_stats['avg_conversion_rate'],
                    total_revenue=round(row.total_revenue, 2),
                    total_views=row.total_videos,
                    avg_revenue_per_video=round(row.avg_revenue_per_video, 2)
                )
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {str(e)}")

        # Return empty stats if query fails
        return DashboardStats(
            total_videos=0,
            analyzed_videos=0,
            avg_script_quality=0.0,
            avg_hook_score=0.0,
            avg_cta_score=0.0,
            avg_conversion_rate=0.0,
            total_revenue=0.0,
            total_views=0,
            avg_revenue_per_video=0.0
        )

    @staticmethod
    def _parse_brand_revenue(brand_revenue_str: str) -> list:
        """Parse 'Brand1:100.50|Brand2:50.25' into [{'brand': 'Brand1', 'revenue': 100.50}, ...]."""
        result = []
        if not brand_revenue_str:
            return result
        for item in brand_revenue_str.split('|'):
            parts = item.rsplit(':', 1)
            if len(parts) == 2:
                try:
                    result.append({'brand': parts[0].strip(), 'revenue': float(parts[1])})
                except ValueError:
                    continue
        return result

    def get_conversion_audit_data(
        self,
        limit: int = 50,
        offset: int = 0,
        channel_code: Optional[str] = None,
        keyword_search: Optional[str] = None,
        silo: Optional[str] = None,
        sort_by: str = 'avg_monthly_revenue',
        sort_dir: str = 'desc'
    ) -> List[Dict]:
        """
        Fetch conversion audit data for all videos in a single efficient query.

        Joins across multiple BigQuery tables to build the audit table:
        - Video metadata (title, channel, description)
        - Keyword & silo from General_info
        - Trailing 90-day avg revenue, views, conversion rate from Metrics_by_Month
        - Thumbnail CTR from YT Analytics (last 90 days)
        - Desc CTR and Pinned CTR from Revenue_Metrics by Link_Placement

        Returns:
            List of dictionaries with audit data per video
        """
        # Validate sort column to prevent SQL injection
        allowed_sorts = {
            'title': 'vi.Video_Title',
            'channel': 'vi.Channel_Code',
            'keyword': 'vi.main_keyword',
            'silo': 'vi.silo',
            'avg_monthly_revenue': 'tr.avg_monthly_revenue',
            'avg_monthly_views': 'tr.avg_monthly_views',
            'epc_90d': 'tr.epc_90d',
            'conversion_rate': 'tr.conversion_rate',
            'desc_ctr': 'desc_ctr',
            'pinned_ctr': 'pinned_ctr',
            'thumbnail_ctr': 'yc.avg_thumbnail_ctr',
            'rank': 'rk.latest_rank',
            'comment_brand': 'cb.comment_affiliate',
            'desc_brand': 'db.desc_affiliate'
        }
        sort_column = allowed_sorts.get(sort_by, 'tr.avg_monthly_revenue')
        sort_direction = 'ASC' if sort_dir.lower() == 'asc' else 'DESC'

        # Build WHERE clause
        where_clauses = ["1=1"]
        params = []

        if channel_code:
            where_clauses.append("UPPER(v.Channel_Code) LIKE @channel_code")
            params.append(bigquery.ScalarQueryParameter("channel_code", "STRING", f"%{channel_code.upper()}%"))

        if keyword_search:
            where_clauses.append("(UPPER(COALESCE(g.main_keyword, '')) LIKE @keyword_search OR UPPER(v.Video_Title) LIKE @keyword_search)")
            params.append(bigquery.ScalarQueryParameter("keyword_search", "STRING", f"%{keyword_search.upper()}%"))

        if silo:
            where_clauses.append("UPPER(COALESCE(g.silo, '')) LIKE @silo_filter")
            params.append(bigquery.ScalarQueryParameter("silo_filter", "STRING", f"%{silo.upper()}%"))

        where_sql = " AND ".join(where_clauses)

        query = f"""
        WITH video_info AS (
            SELECT
                v.video_id,
                v.Video_Title,
                v.Channel_Code,
                v.Video_description,
                g.main_keyword,
                g.silo
            FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2` v
            LEFT JOIN `company-wide-370010.Digibot.Digibot_General_info` g
                ON v.video_id = g.video_id
            WHERE {where_sql}
        ),
        trailing_revenue AS (
            SELECT
                video_id,
                ROUND(AVG(revenue), 2) as avg_monthly_revenue,
                ROUND(AVG(organic_views), 0) as avg_monthly_views,
                SUM(organic_views) as total_views_90d,
                ROUND(SAFE_DIVIDE(SUM(sales), NULLIF(SUM(organic_views), 0)) * 100, 3) as conversion_rate,
                SUM(clicks) as total_clicks,
                SUM(sales) as total_sales,
                ROUND(SUM(revenue), 2) as total_revenue_90d,
                ROUND(SAFE_DIVIDE(SUM(revenue), NULLIF(SUM(clicks), 0)), 2) as epc_90d
            FROM `company-wide-370010.Digibot.Metrics_by_Month`
            WHERE metrics_month_year >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY video_id
        ),
        yt_ctr AS (
            SELECT
                Video_ID,
                ROUND(AVG(impression_CTR), 2) as avg_thumbnail_ctr
            FROM `company-wide-370010.Digibot.Digibot_YT_analytics`
            WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY Video_ID
        ),
        link_clicks AS (
            SELECT
                video_id,
                SUM(IF(LOWER(Link_Placement) LIKE '%desc%', clicks, 0)) as desc_clicks,
                SUM(IF(LOWER(Link_Placement) LIKE '%yt_pc%' OR LOWER(Link_Placement) LIKE '%pinned%', clicks, 0)) as pinned_clicks
            FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
            WHERE metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY video_id
        ),
        latest_rank_date AS (
            SELECT video_id, MAX(rank_date) as max_rank_date
            FROM `company-wide-370010.Digibot.Rank_domination_score`
            GROUP BY video_id
        ),
        rank_data AS (
            SELECT r.video_id, MIN(r.rank) as latest_rank, ld.max_rank_date as rank_date
            FROM `company-wide-370010.Digibot.Rank_domination_score` r
            INNER JOIN latest_rank_date ld ON r.video_id = ld.video_id AND r.rank_date = ld.max_rank_date
            INNER JOIN `company-wide-370010.Digibot.Digibot_General_info` g ON r.video_id = g.video_id
            WHERE LOWER(r.keyword) = LOWER(g.main_keyword)
            GROUP BY r.video_id, ld.max_rank_date
        ),
        desc_brand AS (
            SELECT video_id, Affiliate as desc_affiliate
            FROM (
                SELECT video_id, Affiliate,
                    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY SUM(revenue) DESC) as rn
                FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
                WHERE LOWER(Link_Placement) LIKE '%desc%'
                GROUP BY video_id, Affiliate
            )
            WHERE rn = 1
        ),
        comment_brand AS (
            SELECT video_id, Affiliate as comment_affiliate
            FROM (
                SELECT video_id, Affiliate,
                    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY SUM(revenue) DESC) as rn
                FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
                WHERE LOWER(Link_Placement) LIKE '%yt_pc%' OR LOWER(Link_Placement) LIKE '%pinned%'
                GROUP BY video_id, Affiliate
            )
            WHERE rn = 1
        ),
        brand_revenue_breakdown AS (
            SELECT
                video_id,
                STRING_AGG(
                    CONCAT(Affiliate, ':', CAST(ROUND(total_rev, 2) AS STRING)),
                    '|'
                    ORDER BY total_rev DESC
                ) as brand_revenue
            FROM (
                SELECT
                    video_id,
                    Affiliate,
                    SUM(revenue) as total_rev
                FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
                WHERE metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                    AND Affiliate IS NOT NULL AND TRIM(Affiliate) != ''
                GROUP BY video_id, Affiliate
            )
            GROUP BY video_id
        )
        SELECT
            vi.video_id,
            vi.Video_Title as title,
            vi.Channel_Code as channel,
            vi.Video_description as description,
            vi.main_keyword as keyword,
            vi.silo,
            tr.avg_monthly_revenue,
            tr.avg_monthly_views,
            tr.epc_90d,
            tr.conversion_rate,
            tr.total_clicks,
            tr.total_sales,
            tr.total_revenue_90d,
            yc.avg_thumbnail_ctr as thumbnail_ctr,
            ROUND(SAFE_DIVIDE(lk.desc_clicks, NULLIF(tr.total_views_90d, 0)) * 100, 2) as desc_ctr,
            ROUND(SAFE_DIVIDE(lk.pinned_clicks, NULLIF(tr.total_views_90d, 0)) * 100, 2) as pinned_ctr,
            lk.desc_clicks,
            lk.pinned_clicks,
            tr.total_views_90d,
            rk.latest_rank as rank,
            rk.rank_date,
            db.desc_affiliate,
            cb.comment_affiliate,
            br.brand_revenue
        FROM video_info vi
        LEFT JOIN trailing_revenue tr ON vi.video_id = tr.video_id
        LEFT JOIN yt_ctr yc ON vi.video_id = yc.Video_ID
        LEFT JOIN link_clicks lk ON vi.video_id = lk.video_id
        LEFT JOIN rank_data rk ON vi.video_id = rk.video_id
        LEFT JOIN desc_brand db ON vi.video_id = db.video_id
        LEFT JOIN comment_brand cb ON vi.video_id = cb.video_id
        LEFT JOIN brand_revenue_breakdown br ON vi.video_id = br.video_id
        ORDER BY {sort_column} {sort_direction} NULLS LAST
        LIMIT @limit OFFSET @offset
        """

        params.extend([
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset)
        ])

        try:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            audit_data = []
            for row in results:
                audit_data.append({
                    'video_id': row.video_id,
                    'title': row.title or '',
                    'channel': row.channel or '',
                    'description': row.description or '',
                    'keyword': row.keyword or '',
                    'silo': row.silo or '',
                    'avg_monthly_revenue': float(row.avg_monthly_revenue) if row.avg_monthly_revenue else 0.0,
                    'avg_monthly_views': int(row.avg_monthly_views) if row.avg_monthly_views else 0,
                    'epc_90d': float(row.epc_90d) if row.epc_90d else 0.0,
                    'conversion_rate': float(row.conversion_rate) if row.conversion_rate else 0.0,
                    'total_clicks': int(row.total_clicks) if row.total_clicks else 0,
                    'total_sales': int(row.total_sales) if row.total_sales else 0,
                    'total_revenue_90d': float(row.total_revenue_90d) if row.total_revenue_90d else 0.0,
                    'thumbnail_ctr': float(row.thumbnail_ctr) if row.thumbnail_ctr else 0.0,
                    'desc_ctr': float(row.desc_ctr) if row.desc_ctr else 0.0,
                    'pinned_ctr': float(row.pinned_ctr) if row.pinned_ctr else 0.0,
                    'desc_clicks': int(row.desc_clicks) if row.desc_clicks else 0,
                    'pinned_clicks': int(row.pinned_clicks) if row.pinned_clicks else 0,
                    'total_views_90d': int(row.total_views_90d) if row.total_views_90d else 0,
                    'rank': int(row.rank) if row.rank else None,
                    'rank_date': str(row.rank_date) if row.rank_date else '',
                    'desc_brand': row.desc_affiliate or '',
                    'comment_brand': row.comment_affiliate or '',
                    'brand_revenue': self._parse_brand_revenue(row.brand_revenue) if row.brand_revenue else [],
                })

            logger.info(f"Fetched {len(audit_data)} videos for conversion audit")
            return audit_data

        except Exception as e:
            logger.error(f"Error fetching conversion audit data: {str(e)}")
            return []

    def get_conversion_audit_export(
        self,
        channel_code: Optional[str] = None,
        keyword_search: Optional[str] = None,
        silo: Optional[str] = None,
        sort_by: str = 'avg_monthly_revenue',
        sort_dir: str = 'desc'
    ) -> List[Dict]:
        """Fetch ALL conversion audit data (no pagination) for CSV export."""
        return self.get_conversion_audit_data(
            limit=10000,
            offset=0,
            channel_code=channel_code,
            keyword_search=keyword_search,
            silo=silo,
            sort_by=sort_by,
            sort_dir=sort_dir
        )

    def get_distinct_link_placements(self) -> List[Dict]:
        """Debug: get distinct Link_Placement values and counts from Revenue_Metrics."""
        try:
            query = """
            SELECT Link_Placement, COUNT(*) as cnt, COUNT(DISTINCT video_id) as video_cnt
            FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
            GROUP BY Link_Placement
            ORDER BY cnt DESC
            """
            query_job = self.client.query(query)
            results = query_job.result()
            return [{'placement': row.Link_Placement, 'rows': row.cnt, 'videos': row.video_cnt} for row in results]
        except Exception as e:
            logger.error(f"Error fetching link placements: {str(e)}")
            return []

    def get_all_affiliates(self) -> List[str]:
        """Get all distinct affiliate brand names from Revenue_Metrics."""
        try:
            query = """
            SELECT DISTINCT Affiliate
            FROM `company-wide-370010.Digibot.Revenue_Metrics by date and tracking id`
            WHERE Affiliate IS NOT NULL AND TRIM(Affiliate) != ''
            ORDER BY Affiliate
            """
            query_job = self.client.query(query)
            results = query_job.result()
            affiliates = [row.Affiliate for row in results]
            logger.info(f"Fetched {len(affiliates)} distinct affiliates from BigQuery")
            return affiliates

        except Exception as e:
            logger.error(f"Error getting affiliates: {str(e)}")
            return []

    def get_all_silos(self) -> List[str]:
        """Get all distinct silo values from BigQuery."""
        try:
            query = """
            SELECT DISTINCT silo
            FROM `company-wide-370010.Digibot.Digibot_General_info`
            WHERE silo IS NOT NULL AND TRIM(silo) != ''
            ORDER BY silo
            """
            query_job = self.client.query(query)
            results = query_job.result()
            silos = [row.silo for row in results]
            logger.info(f"Fetched {len(silos)} distinct silos from BigQuery")
            return silos

        except Exception as e:
            logger.error(f"Error getting silos: {str(e)}")
            return []

    def get_all_channels(self) -> List[str]:
        """
        Get all distinct channel codes from BigQuery.

        Returns:
            List of channel codes sorted alphabetically
        """
        try:
            query = """
            SELECT DISTINCT Channel_Code
            FROM `company-wide-370010.1_Youtube_Metrics_Dump.YT_Video_Registration_V2`
            WHERE Channel_Code IS NOT NULL
            ORDER BY Channel_Code
            """

            query_job = self.client.query(query)
            results = query_job.result()

            channels = [row.Channel_Code for row in results]
            logger.info(f"Fetched {len(channels)} distinct channels from BigQuery")
            return channels

        except Exception as e:
            logger.error(f"Error getting channels: {str(e)}")
            return []
