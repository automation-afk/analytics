"""Local database service for storing AI analysis results using SQLite."""
import sqlite3
import logging
from datetime import datetime
from typing import List, Optional
import json
import os

from app.models import (
    ScriptAnalysis, AffiliateRecommendation, DescriptionAnalysis,
    ConversionAnalysis, AnalysisResults, Video, RevenueMetrics
)

logger = logging.getLogger(__name__)


class LocalDBService:
    """Service for managing local SQLite database for analysis results."""

    def __init__(self, db_path: str = None):
        """
        Initialize local database service.

        Args:
            db_path: Path to SQLite database file (defaults to web_app/data/analysis.db)
        """
        if db_path is None:
            # Default to data/analysis.db in web_app directory
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            data_dir = os.path.join(base_dir, 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'analysis.db')

        self.db_path = db_path
        self._init_database()
        logger.info(f"LocalDBService initialized with database: {db_path}")

    def _init_database(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Table 1: Script Analysis
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS script_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            channel_code TEXT,
            analysis_timestamp TEXT NOT NULL,
            script_quality_score REAL,
            hook_effectiveness_score REAL,
            call_to_action_score REAL,
            persuasion_effectiveness_score REAL,
            user_intent_match_score REAL,
            persuasion_techniques TEXT,  -- JSON array
            key_strengths TEXT,  -- JSON array
            improvement_areas TEXT,  -- JSON array
            target_audience TEXT,
            content_value_score REAL,
            identified_intent TEXT,
            has_clear_intro INTEGER,
            has_clear_cta INTEGER,
            problem_solution_structure INTEGER,
            readability_score REAL,
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Table 2: Affiliate Recommendations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS affiliate_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            recommendation_timestamp TEXT NOT NULL,
            product_rank INTEGER,
            product_name TEXT,
            product_category TEXT,
            relevance_score REAL,
            conversion_probability REAL,
            recommendation_reasoning TEXT,
            where_to_mention TEXT,
            mentioned_in_video INTEGER,
            amazon_asin TEXT,
            price_range TEXT
        )
        """)

        # Table 3: Description Analysis
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS description_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            analysis_timestamp TEXT NOT NULL,
            cta_effectiveness_score REAL,
            description_quality_score REAL,
            seo_score REAL,
            total_links INTEGER,
            affiliate_links INTEGER,
            link_positioning_score REAL,
            has_clear_cta INTEGER,
            optimization_suggestions TEXT,  -- JSON array
            missing_elements TEXT,  -- JSON array
            strengths TEXT,  -- JSON array
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Table 4: Conversion Analysis
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversion_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            analysis_timestamp TEXT NOT NULL,
            metrics_date TEXT,
            revenue REAL,
            clicks INTEGER,
            sales INTEGER,
            views INTEGER,
            conversion_rate REAL,
            revenue_per_click REAL,
            revenue_per_1k_views REAL,
            conversion_drivers TEXT,  -- JSON array
            underperformance_reasons TEXT,  -- JSON array
            recommendations TEXT,  -- JSON array
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_script_video_id ON script_analysis(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_video_id ON affiliate_recommendations(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_video_id ON description_analysis(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_conversion_video_id ON conversion_analysis(video_id)")

        conn.commit()
        conn.close()
        logger.info("Database tables initialized successfully")

    def store_script_analysis(self, analysis: ScriptAnalysis) -> bool:
        """Store script analysis results to local database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            INSERT OR REPLACE INTO script_analysis (
                video_id, channel_code, analysis_timestamp,
                script_quality_score, hook_effectiveness_score, call_to_action_score,
                persuasion_effectiveness_score, user_intent_match_score,
                persuasion_techniques, key_strengths, improvement_areas,
                target_audience, content_value_score, identified_intent,
                has_clear_intro, has_clear_cta, problem_solution_structure,
                readability_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.video_id,
                analysis.channel_code,
                analysis.analysis_timestamp.isoformat(),
                analysis.script_quality_score,
                analysis.hook_effectiveness_score,
                analysis.call_to_action_score,
                analysis.persuasion_effectiveness_score,
                analysis.user_intent_match_score,
                json.dumps(analysis.persuasion_techniques),
                json.dumps(analysis.key_strengths),
                json.dumps(analysis.improvement_areas),
                analysis.target_audience,
                analysis.content_value_score,
                analysis.identified_intent,
                1 if analysis.has_clear_intro else 0,
                1 if analysis.has_clear_cta else 0,
                1 if analysis.problem_solution_structure else 0,
                analysis.readability_score
            ))

            conn.commit()
            conn.close()
            logger.info(f"Successfully stored script analysis for video: {analysis.video_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing script analysis: {str(e)}")
            return False

    def store_affiliate_recommendations(self, recommendations: List[AffiliateRecommendation]) -> bool:
        """Store affiliate recommendations to local database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Delete old recommendations for this video
            if recommendations:
                cursor.execute(
                    "DELETE FROM affiliate_recommendations WHERE video_id = ?",
                    (recommendations[0].video_id,)
                )

            # Insert new recommendations
            for rec in recommendations:
                cursor.execute("""
                INSERT INTO affiliate_recommendations (
                    video_id, recommendation_timestamp, product_rank,
                    product_name, product_category, relevance_score,
                    conversion_probability, recommendation_reasoning,
                    where_to_mention, mentioned_in_video, amazon_asin, price_range
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec.video_id,
                    rec.recommendation_timestamp.isoformat(),
                    rec.product_rank,
                    rec.product_name,
                    rec.product_category,
                    rec.relevance_score,
                    rec.conversion_probability,
                    rec.recommendation_reasoning,
                    rec.where_to_mention,
                    1 if rec.mentioned_in_video else 0,
                    rec.amazon_asin,
                    rec.price_range
                ))

            conn.commit()
            conn.close()
            logger.info(f"Successfully stored {len(recommendations)} affiliate recommendations")
            return True

        except Exception as e:
            logger.error(f"Error storing affiliate recommendations: {str(e)}")
            return False

    def store_description_analysis(self, analysis: DescriptionAnalysis) -> bool:
        """Store description analysis to local database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            INSERT OR REPLACE INTO description_analysis (
                video_id, analysis_timestamp, cta_effectiveness_score,
                description_quality_score, seo_score, total_links,
                affiliate_links, link_positioning_score, has_clear_cta,
                optimization_suggestions, missing_elements, strengths
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.video_id,
                analysis.analysis_timestamp.isoformat(),
                analysis.cta_effectiveness_score,
                analysis.description_quality_score,
                analysis.seo_score,
                analysis.total_links,
                analysis.affiliate_links,
                analysis.link_positioning_score,
                1 if analysis.has_clear_cta else 0,
                json.dumps(analysis.optimization_suggestions),
                json.dumps(analysis.missing_elements),
                json.dumps(analysis.strengths)
            ))

            conn.commit()
            conn.close()
            logger.info(f"Successfully stored description analysis for video: {analysis.video_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing description analysis: {str(e)}")
            return False

    def store_conversion_analysis(self, analysis: ConversionAnalysis) -> bool:
        """Store conversion analysis to local database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            INSERT OR REPLACE INTO conversion_analysis (
                video_id, analysis_timestamp, metrics_date,
                revenue, clicks, sales, views,
                conversion_rate, revenue_per_click, revenue_per_1k_views,
                conversion_drivers, underperformance_reasons, recommendations
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.video_id,
                analysis.analysis_timestamp.isoformat(),
                analysis.metrics_date.isoformat() if analysis.metrics_date else None,
                analysis.revenue,
                analysis.clicks,
                analysis.sales,
                analysis.views,
                analysis.conversion_rate,
                analysis.revenue_per_click,
                analysis.revenue_per_1k_views,
                json.dumps(analysis.conversion_drivers),
                json.dumps(analysis.underperformance_reasons),
                json.dumps(analysis.recommendations)
            ))

            conn.commit()
            conn.close()
            logger.info(f"Successfully stored conversion analysis for video: {analysis.video_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing conversion analysis: {str(e)}")
            return False

    def get_script_analysis(self, video_id: str) -> Optional[ScriptAnalysis]:
        """Fetch latest script analysis for a video."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM script_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                return ScriptAnalysis(
                    video_id=row['video_id'],
                    channel_code=row['channel_code'],
                    analysis_timestamp=datetime.fromisoformat(row['analysis_timestamp']),
                    script_quality_score=row['script_quality_score'],
                    hook_effectiveness_score=row['hook_effectiveness_score'],
                    call_to_action_score=row['call_to_action_score'],
                    persuasion_effectiveness_score=row['persuasion_effectiveness_score'],
                    user_intent_match_score=row['user_intent_match_score'],
                    persuasion_techniques=json.loads(row['persuasion_techniques']) if row['persuasion_techniques'] else [],
                    key_strengths=json.loads(row['key_strengths']) if row['key_strengths'] else [],
                    improvement_areas=json.loads(row['improvement_areas']) if row['improvement_areas'] else [],
                    target_audience=row['target_audience'],
                    content_value_score=row['content_value_score'],
                    identified_intent=row['identified_intent'],
                    has_clear_intro=bool(row['has_clear_intro']),
                    has_clear_cta=bool(row['has_clear_cta']),
                    problem_solution_structure=bool(row['problem_solution_structure']),
                    readability_score=row['readability_score']
                )
            return None

        except Exception as e:
            logger.error(f"Error fetching script analysis: {str(e)}")
            return None

    def get_affiliate_recommendations(self, video_id: str) -> List[AffiliateRecommendation]:
        """Fetch latest affiliate recommendations for a video."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM affiliate_recommendations
            WHERE video_id = ?
            ORDER BY recommendation_timestamp DESC, product_rank ASC
            LIMIT 5
            """, (video_id,))

            rows = cursor.fetchall()
            conn.close()

            recommendations = []
            for row in rows:
                recommendations.append(AffiliateRecommendation(
                    video_id=row['video_id'],
                    recommendation_timestamp=datetime.fromisoformat(row['recommendation_timestamp']),
                    product_rank=row['product_rank'],
                    product_name=row['product_name'],
                    product_category=row['product_category'],
                    relevance_score=row['relevance_score'],
                    conversion_probability=row['conversion_probability'],
                    recommendation_reasoning=row['recommendation_reasoning'],
                    where_to_mention=row['where_to_mention'],
                    mentioned_in_video=bool(row['mentioned_in_video']),
                    amazon_asin=row['amazon_asin'],
                    price_range=row['price_range']
                ))

            return recommendations

        except Exception as e:
            logger.error(f"Error fetching affiliate recommendations: {str(e)}")
            return []

    def get_description_analysis(self, video_id: str) -> Optional[DescriptionAnalysis]:
        """Fetch latest description analysis for a video."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM description_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                return DescriptionAnalysis(
                    video_id=row['video_id'],
                    analysis_timestamp=datetime.fromisoformat(row['analysis_timestamp']),
                    cta_effectiveness_score=row['cta_effectiveness_score'],
                    description_quality_score=row['description_quality_score'],
                    seo_score=row['seo_score'],
                    total_links=row['total_links'],
                    affiliate_links=row['affiliate_links'],
                    link_positioning_score=row['link_positioning_score'],
                    has_clear_cta=bool(row['has_clear_cta']),
                    optimization_suggestions=json.loads(row['optimization_suggestions']) if row['optimization_suggestions'] else [],
                    missing_elements=json.loads(row['missing_elements']) if row['missing_elements'] else [],
                    strengths=json.loads(row['strengths']) if row['strengths'] else []
                )
            return None

        except Exception as e:
            logger.error(f"Error fetching description analysis: {str(e)}")
            return None

    def get_conversion_analysis(self, video_id: str) -> Optional[ConversionAnalysis]:
        """Fetch latest conversion analysis for a video."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM conversion_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                return ConversionAnalysis(
                    video_id=row['video_id'],
                    analysis_timestamp=datetime.fromisoformat(row['analysis_timestamp']),
                    metrics_date=datetime.fromisoformat(row['metrics_date']).date() if row['metrics_date'] else None,
                    revenue=row['revenue'],
                    clicks=row['clicks'],
                    sales=row['sales'],
                    views=row['views'],
                    conversion_rate=row['conversion_rate'],
                    revenue_per_click=row['revenue_per_click'],
                    revenue_per_1k_views=row['revenue_per_1k_views'],
                    conversion_drivers=json.loads(row['conversion_drivers']) if row['conversion_drivers'] else [],
                    underperformance_reasons=json.loads(row['underperformance_reasons']) if row['underperformance_reasons'] else [],
                    recommendations=json.loads(row['recommendations']) if row['recommendations'] else []
                )
            return None

        except Exception as e:
            logger.error(f"Error fetching conversion analysis: {str(e)}")
            return None

    def has_analysis(self, video_id: str) -> bool:
        """Check if a video has any analysis results."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            SELECT 1 FROM script_analysis WHERE video_id = ?
            UNION
            SELECT 1 FROM description_analysis WHERE video_id = ?
            UNION
            SELECT 1 FROM conversion_analysis WHERE video_id = ?
            LIMIT 1
            """, (video_id, video_id, video_id))

            result = cursor.fetchone()
            conn.close()

            return result is not None

        except Exception as e:
            logger.error(f"Error checking analysis: {str(e)}")
            return False
