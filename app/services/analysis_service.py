"""Analysis service for orchestrating AI analysis workflows."""
import logging
from datetime import datetime
from typing import List, Optional
import time

# Import analyzers from local services
from app.services.content_analyzer import ContentAnalyzer
from app.services.description_analyzer import DescriptionAnalyzer
from app.services.affiliate_recommender import AffiliateRecommender
from app.services.conversion_analyzer import ConversionAnalyzer

from app.models import (
    ScriptAnalysis, AffiliateRecommendation, DescriptionAnalysis,
    ConversionAnalysis, AnalysisResults
)

logger = logging.getLogger(__name__)


class AnalysisService:
    """Service for orchestrating AI analysis of YouTube videos."""

    def __init__(self, bigquery_service, anthropic_api_key: str):
        """
        Initialize analysis service.

        Args:
            bigquery_service: BigQueryService instance
            anthropic_api_key: Anthropic API key for Claude
        """
        self.bigquery = bigquery_service
        self.anthropic_api_key = anthropic_api_key

        # Initialize analyzers
        self.content_analyzer = ContentAnalyzer(anthropic_api_key)
        self.description_analyzer = DescriptionAnalyzer(anthropic_api_key)
        self.affiliate_recommender = AffiliateRecommender(anthropic_api_key)
        self.conversion_analyzer = ConversionAnalyzer(anthropic_api_key)

        logger.info("AnalysisService initialized with all analyzers")

    def analyze_video(
        self,
        video_id: str,
        analysis_types: List[str],
        progress_callback: callable = None
    ) -> AnalysisResults:
        """
        Run AI analysis on a single video.

        Args:
            video_id: YouTube video ID
            analysis_types: List of analysis types to run
                          ['script', 'description', 'affiliate', 'conversion']
            progress_callback: Optional callback(step, progress, message) for progress updates

        Returns:
            AnalysisResults object

        Raises:
            Exception if video not found or analysis fails
        """
        def update_progress(step: str, progress: int, message: str = ""):
            """Update progress via callback if provided."""
            if progress_callback:
                try:
                    progress_callback(step, progress, message)
                except Exception as e:
                    logger.warning(f"Progress callback error: {e}")

        logger.info(f"Starting analysis for video: {video_id}")
        logger.info(f"Analysis types: {analysis_types}")

        # Fetch video data from BigQuery
        video = self.bigquery.get_video_by_id(video_id)
        if not video:
            raise Exception(f"Video not found: {video_id}")

        # Fetch transcript - check local DB first, then BigQuery
        transcript = None
        local_transcript = None

        # Check local DB for transcript (from our transcription service)
        if self.bigquery.local_db:
            local_transcript = self.bigquery.local_db.get_transcript(video_id)
            if local_transcript:
                transcript = local_transcript.get('transcript')
                logger.info(f"Using local transcript for {video_id} ({local_transcript.get('word_count')} words)")

                # Also get description from local transcript if video doesn't have one
                if (not video.description or not video.description.strip()) and local_transcript.get('description'):
                    video.description = local_transcript.get('description')
                    logger.info(f"Using description from local transcript for {video_id}")

        # Fallback to BigQuery transcript
        if not transcript:
            transcript = self.bigquery.get_transcript(video_id)

        if not transcript and 'script' in analysis_types:
            logger.warning(f"No transcript found for video: {video_id}")
            # Continue anyway - some analyses might not need transcript

        # Fetch revenue metrics
        revenue_metrics = self.bigquery.get_revenue_metrics(video_id)

        # Initialize results
        script_analysis = None
        description_analysis = None
        affiliate_recommendations = []
        conversion_analysis = None

        # Run analyses based on requested types
        timestamp = datetime.now()
        update_progress('fetching', 10, 'Video data loaded, starting analysis...')

        # 1. Script Quality Analysis
        if 'script' in analysis_types and transcript:
            update_progress('script', 15, 'Analyzing script quality...')
            try:
                logger.info(f"Running script analysis for {video_id}")
                result = self.content_analyzer.analyze_script_quality(
                    transcript=transcript,
                    title=video.title,
                    description=video.description
                )

                # Convert to ScriptAnalysis model
                script_analysis = ScriptAnalysis(
                    video_id=video_id,
                    channel_code=video.channel_code,
                    analysis_timestamp=timestamp,
                    script_quality_score=result.get('script_quality_score', 0.0),
                    hook_effectiveness_score=result.get('hook_effectiveness_score', 0.0),
                    call_to_action_score=result.get('call_to_action_score', 0.0),
                    persuasion_effectiveness_score=result.get('persuasion_effectiveness_score', 0.0),
                    user_intent_match_score=result.get('user_intent_match_score', 0.0),
                    persuasion_techniques=result.get('persuasion_techniques', []),
                    key_strengths=result.get('key_strengths', []),
                    improvement_areas=result.get('improvement_areas', []),
                    target_audience=result.get('target_audience', ''),
                    content_value_score=result.get('content_value_score', 0.0),
                    identified_intent=result.get('identified_intent', ''),
                    has_clear_intro=result.get('has_clear_intro', False),
                    has_clear_cta=result.get('has_clear_cta', False),
                    problem_solution_structure=result.get('problem_solution_structure', False),
                    readability_score=result.get('readability_score', 0.0)
                )

                # Store to BigQuery
                self.bigquery.store_script_analysis(script_analysis)
                logger.info(f"Script analysis completed and stored for {video_id}")
                update_progress('script', 35, 'Script analysis complete')

            except Exception as e:
                logger.error(f"Error in script analysis: {str(e)}")

            # Rate limiting
            time.sleep(2)

        # 2. Description Analysis
        if 'description' in analysis_types:
            update_progress('description', 40, 'Analyzing description...')
            try:
                # Fetch YT Analytics data from BigQuery (last 90 days) - always try this
                update_progress('description', 42, 'Fetching YT Analytics data...')
                yt_analytics_summary = self.bigquery.get_yt_analytics_summary(video_id, days=90)
                logger.info(f"YT Analytics for {video_id}: {yt_analytics_summary.get('total_views', 0)} views, {len(yt_analytics_summary.get('by_traffic_source', []))} traffic sources")

                # Check if we have a description to analyze
                if video.description and video.description.strip():
                    logger.info(f"Running description analysis for {video_id} (description length: {len(video.description)})")

                    # Run AI description analysis
                    update_progress('description', 45, 'Running AI description analysis...')
                    result = self.description_analyzer.analyze(
                        description=video.description,
                        title=video.title,
                        yt_analytics=yt_analytics_summary  # Pass YT Analytics data
                    )
                else:
                    logger.warning(f"No description available for {video_id}, storing YT Analytics data only")
                    # Create minimal result with just YT Analytics data
                    result = {
                        'cta_effectiveness_score': 0.0,
                        'description_quality_score': 0.0,
                        'seo_score': 0.0,
                        'total_links': 0,
                        'affiliate_links': 0,
                        'link_positioning_score': 0.0,
                        'has_clear_cta': False,
                        'optimization_suggestions': ['No description available - run Transcribe & Analyze to fetch from YouTube'],
                        'missing_elements': ['Video description not available'],
                        'strengths': []
                    }

                # Convert to DescriptionAnalysis model with YT Analytics data
                description_analysis = DescriptionAnalysis(
                    video_id=video_id,
                    analysis_timestamp=timestamp,
                    cta_effectiveness_score=result.get('cta_effectiveness_score', 0.0),
                    description_quality_score=result.get('description_quality_score', 0.0),
                    seo_score=result.get('seo_score', 0.0),
                    total_links=result.get('total_links', 0),
                    affiliate_links=result.get('affiliate_links', 0),
                    link_positioning_score=result.get('link_positioning_score', 0.0),
                    has_clear_cta=result.get('has_clear_cta', False),
                    optimization_suggestions=result.get('optimization_suggestions', []),
                    missing_elements=result.get('missing_elements', []),
                    strengths=result.get('strengths', []),
                    # YT Analytics data from BigQuery
                    yt_total_views=yt_analytics_summary.get('total_views', 0),
                    yt_total_impressions=yt_analytics_summary.get('total_impressions', 0),
                    yt_overall_ctr=yt_analytics_summary.get('overall_ctr', 0.0),
                    yt_by_traffic_source=yt_analytics_summary.get('by_traffic_source', []),
                    main_keyword=yt_analytics_summary.get('main_keyword', ''),
                    silo=yt_analytics_summary.get('silo', '')
                )

                # Store to local database
                self.bigquery.store_description_analysis(description_analysis)
                logger.info(f"Description analysis completed and stored for {video_id}")
                update_progress('description', 55, 'Description analysis complete')

            except Exception as e:
                logger.error(f"Error in description analysis: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

            # Rate limiting
            time.sleep(2)

        # 3. Affiliate Product Recommendations
        if 'affiliate' in analysis_types and transcript:
            update_progress('affiliate', 60, 'Generating affiliate recommendations...')
            try:
                logger.info(f"Running affiliate recommendations for {video_id}")
                results = self.affiliate_recommender.recommend_products(
                    transcript=transcript,
                    title=video.title,
                    description=video.description,
                    top_n=5
                )

                # Convert to AffiliateRecommendation models
                for idx, rec in enumerate(results, 1):
                    affiliate_recommendations.append(AffiliateRecommendation(
                        video_id=video_id,
                        recommendation_timestamp=timestamp,
                        product_rank=idx,
                        product_name=rec.get('product_name', ''),
                        product_category=rec.get('product_category', ''),
                        relevance_score=rec.get('relevance_score', 0.0),
                        conversion_probability=rec.get('conversion_probability', 0.0),
                        recommendation_reasoning=rec.get('recommendation_reasoning', ''),
                        where_to_mention=rec.get('where_to_mention', ''),
                        mentioned_in_video=rec.get('mentioned_in_video', False),
                        amazon_asin=rec.get('amazon_asin'),
                        price_range=rec.get('price_range')
                    ))

                # Store to BigQuery
                if affiliate_recommendations:
                    self.bigquery.store_affiliate_recommendations(affiliate_recommendations)
                    logger.info(f"Affiliate recommendations completed and stored for {video_id}")
                    update_progress('affiliate', 75, 'Affiliate recommendations complete')

            except Exception as e:
                logger.error(f"Error in affiliate recommendations: {str(e)}")

            # Rate limiting
            time.sleep(2)

        # 4. Conversion Analysis (AI-powered with Claude)
        if 'conversion' in analysis_types:
            update_progress('conversion', 80, 'Analyzing conversion metrics...')
            try:
                logger.info(f"Running AI-powered conversion analysis for {video_id}")

                if revenue_metrics and revenue_metrics.clicks > 0:
                    # Use AI to analyze conversion drivers (works with or without transcript)
                    ai_analysis = self.conversion_analyzer.analyze_conversion_drivers(
                        transcript=transcript or "",
                        title=video.title,
                        description=video.description or "",
                        revenue=revenue_metrics.revenue,
                        clicks=revenue_metrics.clicks,
                        sales=revenue_metrics.sales,
                        views=revenue_metrics.organic_views,
                        script_quality_score=script_analysis.script_quality_score if script_analysis else None,
                        cta_score=script_analysis.call_to_action_score if script_analysis else None
                    )

                    conversion_analysis = ConversionAnalysis(
                        video_id=video_id,
                        analysis_timestamp=timestamp,
                        metrics_date=revenue_metrics.metrics_date,
                        revenue=revenue_metrics.revenue,
                        clicks=revenue_metrics.clicks,
                        sales=revenue_metrics.sales,
                        views=revenue_metrics.organic_views,
                        conversion_rate=revenue_metrics.conversion_rate,
                        revenue_per_click=revenue_metrics.revenue_per_click,
                        revenue_per_1k_views=revenue_metrics.revenue_per_1k_views,
                        conversion_drivers=ai_analysis.get('conversion_drivers', []),
                        underperformance_reasons=ai_analysis.get('underperformance_reasons', []),
                        recommendations=ai_analysis.get('recommendations', [])
                    )
                    logger.info(f"Conversion analysis complete: {len(ai_analysis.get('conversion_drivers', []))} drivers identified")
                else:
                    # Create basic conversion analysis when no revenue data
                    from datetime import date
                    if revenue_metrics:
                        # Has revenue but no clicks
                        conversion_analysis = ConversionAnalysis(
                            video_id=video_id,
                            analysis_timestamp=timestamp,
                            metrics_date=revenue_metrics.metrics_date,
                            revenue=revenue_metrics.revenue,
                            clicks=revenue_metrics.clicks,
                            sales=revenue_metrics.sales,
                            views=revenue_metrics.organic_views,
                            conversion_rate=revenue_metrics.conversion_rate,
                            revenue_per_click=revenue_metrics.revenue_per_click,
                            revenue_per_1k_views=revenue_metrics.revenue_per_1k_views,
                            conversion_drivers=["No affiliate clicks yet - analysis requires click data"],
                            underperformance_reasons=[],
                            recommendations=["Ensure affiliate links are properly placed in description"]
                        )
                    else:
                        # No revenue data at all
                        conversion_analysis = ConversionAnalysis(
                            video_id=video_id,
                            analysis_timestamp=timestamp,
                            metrics_date=date.today(),
                            revenue=0.0,
                            clicks=0,
                            sales=0,
                            views=0,
                            conversion_rate=0.0,
                            revenue_per_click=0.0,
                            revenue_per_1k_views=0.0,
                            conversion_drivers=["No revenue data available yet"],
                            underperformance_reasons=["Video has not generated revenue data or data not synced to BigQuery"],
                            recommendations=["Check back after video generates affiliate clicks/revenue"]
                        )

                # Store to local database
                self.bigquery.store_conversion_analysis(conversion_analysis)
                logger.info(f"Conversion analysis completed and stored for {video_id}")
                update_progress('conversion', 95, 'Conversion analysis complete')

            except Exception as e:
                logger.error(f"Error in conversion analysis: {str(e)}")

        update_progress('saving', 98, 'Finalizing results...')
        # Return combined results
        return AnalysisResults(
            video=video,
            revenue_metrics=revenue_metrics,
            script_analysis=script_analysis,
            affiliate_recommendations=affiliate_recommendations,
            description_analysis=description_analysis,
            conversion_analysis=conversion_analysis
        )


    def batch_analyze(
        self,
        video_ids: List[str],
        analysis_types: List[str],
        rate_limit_seconds: int = 2
    ) -> List[AnalysisResults]:
        """
        Run analysis on multiple videos.

        Args:
            video_ids: List of YouTube video IDs
            analysis_types: List of analysis types to run
            rate_limit_seconds: Seconds to wait between analyses

        Returns:
            List of AnalysisResults
        """
        results = []

        for video_id in video_ids:
            try:
                result = self.analyze_video(video_id, analysis_types)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to analyze video {video_id}: {str(e)}")

            # Rate limiting between videos
            time.sleep(rate_limit_seconds)

        return results
