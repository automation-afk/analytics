"""BigQuery service for reading and writing YouTube analytics data."""
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from app.models import (
    Video, RevenueMetrics, VideoTranscript, ScriptAnalysis,
    AffiliateRecommendation, DescriptionAnalysis, ConversionAnalysis,
    AnalysisResults, DashboardStats
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
        # Include both BigQuery and Drive scopes to access Drive-backed external tables
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
            # Case-insensitive partial match for video ID
            query += " AND UPPER(v.video_id) LIKE @video_id"
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

            return Video(
                video_id=row.video_id,
                channel_code=row.channel_code,
                title=row.title,
                published_date=row.published_date,
                video_url=video_url,
                description=row.description,
                has_analysis=False,
                latest_analysis_date=None
            )

        logger.warning(f"Video not found: {video_id}")
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

    def get_revenue_metrics(self, video_id: str) -> Optional[RevenueMetrics]:
        """
        Fetch aggregated revenue metrics for a video across all months.

        Args:
            video_id: YouTube video ID

        Returns:
            RevenueMetrics object with totals or None if not found
        """
        query = """
        SELECT
            m.video_id,
            g.presenter as channel,
            MAX(m.metrics_month_year) as latest_month,
            SUM(m.revenue) as total_revenue,
            SUM(m.clicks) as total_clicks,
            SUM(m.sales) as total_sales,
            SUM(m.organic_views) as total_views
        FROM `company-wide-370010.Digibot.Metrics_by_Month` m
        LEFT JOIN `company-wide-370010.Digibot.Digibot_General_info` g
          ON m.video_id = g.video_id
        WHERE m.video_id = @video_id
        GROUP BY m.video_id, g.presenter
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

            # Calculate derived metrics
            conversion_rate = (sales / clicks * 100) if clicks > 0 else 0.0
            revenue_per_click = (revenue / clicks) if clicks > 0 else 0.0
            revenue_per_1k_views = (revenue / views * 1000) if views > 0 else 0.0

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
                revenue_per_1k_views=revenue_per_1k_views
            )

        logger.warning(f"Revenue metrics not found for video: {video_id}")
        return None

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

        return AnalysisResults(
            video=video,
            revenue_metrics=revenue_metrics,
            script_analysis=script_analysis,
            affiliate_recommendations=affiliate_recs,
            description_analysis=description_analysis,
            conversion_analysis=conversion_analysis
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
                'avg_cta_score': 0.0
            }

        import sqlite3
        try:
            conn = sqlite3.connect(self.local_db.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            SELECT
                COUNT(DISTINCT video_id) as analyzed_videos,
                AVG(script_quality_score) as avg_script_quality,
                AVG(hook_effectiveness_score) as avg_hook_score,
                AVG(call_to_action_score) as avg_cta_score
            FROM script_analysis
            """)

            row = cursor.fetchone()
            conn.close()

            if row and row[0]:
                return {
                    'analyzed_videos': row[0] or 0,
                    'avg_script_quality': round(row[1], 1) if row[1] else 0.0,
                    'avg_hook_score': round(row[2], 1) if row[2] else 0.0,
                    'avg_cta_score': round(row[3], 1) if row[3] else 0.0
                }
        except Exception as e:
            logger.error(f"Error getting local analysis stats: {str(e)}")

        return {
            'analyzed_videos': 0,
            'avg_script_quality': 0.0,
            'avg_hook_score': 0.0,
            'avg_cta_score': 0.0
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
                    avg_conversion_rate=0.0,
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
