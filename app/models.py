"""Data models for YouTube analytics dashboard."""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional


@dataclass
class Video:
    """Video metadata and basic information."""
    video_id: str
    channel_code: str
    title: str
    published_date: datetime
    video_url: str
    description: str
    views: int = 0
    has_analysis: bool = False
    latest_analysis_date: Optional[datetime] = None


@dataclass
class RevenueMetrics:
    """Revenue and performance metrics for a video."""
    video_id: str
    channel: str
    metrics_date: date
    revenue: float = 0.0
    clicks: int = 0
    sales: int = 0
    organic_views: int = 0
    conversion_rate: float = 0.0
    revenue_per_click: float = 0.0
    revenue_per_1k_views: float = 0.0
    impression_ctr: float = 0.0  # CTR from YT Analytics (already in %)


@dataclass
class VideoTranscript:
    """Video transcript information."""
    video_id: str
    transcript_text: str
    language: str = 'en'


@dataclass
class ScriptAnalysis:
    """AI analysis results for video script quality."""
    video_id: str
    channel_code: str
    analysis_timestamp: datetime
    script_quality_score: float
    hook_effectiveness_score: float
    call_to_action_score: float
    persuasion_effectiveness_score: float
    user_intent_match_score: float
    persuasion_techniques: List[str] = field(default_factory=list)
    key_strengths: List[str] = field(default_factory=list)
    improvement_areas: List[str] = field(default_factory=list)
    target_audience: str = ""
    content_value_score: float = 0.0
    identified_intent: str = ""
    has_clear_intro: bool = False
    has_clear_cta: bool = False
    problem_solution_structure: bool = False
    readability_score: float = 0.0


@dataclass
class AffiliateRecommendation:
    """AI-generated affiliate product recommendation."""
    video_id: str
    recommendation_timestamp: datetime
    product_rank: int
    product_name: str
    product_category: str
    relevance_score: float
    conversion_probability: float
    recommendation_reasoning: str
    where_to_mention: str
    mentioned_in_video: bool = False
    amazon_asin: Optional[str] = None
    price_range: Optional[str] = None


@dataclass
class DescriptionAnalysis:
    """AI analysis results for video description CTR."""
    video_id: str
    analysis_timestamp: datetime
    cta_effectiveness_score: float
    description_quality_score: float
    seo_score: float
    total_links: int = 0
    affiliate_links: int = 0
    link_positioning_score: float = 0.0
    has_clear_cta: bool = False
    optimization_suggestions: List[str] = field(default_factory=list)
    missing_elements: List[str] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)
    # YT Analytics data (from BigQuery - last 90 days)
    yt_total_views: int = 0
    yt_total_impressions: int = 0
    yt_overall_ctr: float = 0.0
    yt_by_traffic_source: List[dict] = field(default_factory=list)  # [{traffic_source, views, impressions, avg_ctr, avg_view_percentage}]
    main_keyword: str = ""
    silo: str = ""


@dataclass
class ConversionAnalysis:
    """AI analysis of conversion rate drivers."""
    video_id: str
    analysis_timestamp: datetime
    metrics_date: date
    revenue: float
    clicks: int
    sales: int
    views: int
    conversion_rate: float
    revenue_per_click: float
    revenue_per_1k_views: float
    conversion_drivers: List[str] = field(default_factory=list)
    underperformance_reasons: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class AffiliatePerformance:
    """Real affiliate performance data from BigQuery Revenue_Metrics table."""
    video_id: str
    tracking_id: str
    platform: str
    affiliate: str
    link_placement: str
    total_revenue: float = 0.0
    total_clicks: int = 0
    total_sales: int = 0
    conversion_rate: float = 0.0
    revenue_per_click: float = 0.0


@dataclass
class AnalysisResults:
    """Combined analysis results for a video."""
    video: Video
    revenue_metrics: Optional[RevenueMetrics] = None
    script_analysis: Optional[ScriptAnalysis] = None
    affiliate_recommendations: List[AffiliateRecommendation] = field(default_factory=list)
    description_analysis: Optional[DescriptionAnalysis] = None
    conversion_analysis: Optional[ConversionAnalysis] = None
    affiliate_performance: List[AffiliatePerformance] = field(default_factory=list)
    existing_links_analysis: Optional[Dict] = None


@dataclass
class DashboardStats:
    """Dashboard overview statistics."""
    total_videos: int
    analyzed_videos: int
    avg_script_quality: float
    avg_hook_score: float
    avg_cta_score: float
    avg_conversion_rate: float
    total_revenue: float
    total_views: int
    avg_revenue_per_video: float


@dataclass
class AnalysisJob:
    """Background analysis job tracking."""
    job_id: str
    status: str  # pending, running, completed, failed
    video_ids: List[str]
    analysis_types: List[str]
    progress: int  # 0-100
    current_video: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    total_videos: int = 0
    processed_videos: int = 0
