"""Local database service for storing AI analysis results using SQLite or PostgreSQL."""
import logging
from datetime import datetime
from typing import List, Optional
import json
import os

from app.models import (
    ScriptAnalysis, AffiliateRecommendation, DescriptionAnalysis,
    ConversionAnalysis, AnalysisResults, Video, RevenueMetrics,
    ScriptScore, GateCheckResult, ApprovedBrand, Partner
)

logger = logging.getLogger(__name__)


class LocalDBService:
    """Service for managing local database for analysis results.

    Supports both SQLite (local development) and PostgreSQL (Railway production).
    Uses DATABASE_URL environment variable to determine which to use.
    """

    def __init__(self, db_path: str = None):
        """
        Initialize database service.

        Args:
            db_path: Path to SQLite database file (only used if DATABASE_URL not set)
        """
        # Check for PostgreSQL DATABASE_URL first (Railway provides this)
        self.database_url = os.getenv('DATABASE_URL')
        self.use_postgres = bool(self.database_url)

        if self.use_postgres:
            # PostgreSQL mode
            self.db_path = None
            logger.info(f"Using PostgreSQL database")
        else:
            # SQLite mode (local development)
            if db_path is None:
                db_path = os.getenv('DATABASE_PATH')

                if db_path is None:
                    if os.path.exists('/app'):
                        data_dir = '/app/data'
                    else:
                        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                        data_dir = os.path.join(base_dir, 'data')

                    os.makedirs(data_dir, exist_ok=True)
                    db_path = os.path.join(data_dir, 'analysis.db')

            self.db_path = db_path
            logger.info(f"Using SQLite database: {db_path}")

        self._init_database()
        logger.info(f"LocalDBService initialized ({'PostgreSQL' if self.use_postgres else 'SQLite'})")

    def _get_connection(self):
        """Get database connection based on configuration."""
        if self.use_postgres:
            import psycopg2
            return psycopg2.connect(self.database_url)
        else:
            import sqlite3
            return sqlite3.connect(self.db_path)

    def _get_placeholder(self):
        """Get parameter placeholder for queries."""
        return "%s" if self.use_postgres else "?"

    def _init_database(self):
        """Create database tables if they don't exist."""
        conn = self._get_connection()
        cursor = conn.cursor()

        p = self._get_placeholder()

        # Determine auto-increment syntax
        if self.use_postgres:
            auto_id = "SERIAL PRIMARY KEY"
            int_type = "INTEGER"
            text_type = "TEXT"
            real_type = "REAL"
        else:
            auto_id = "INTEGER PRIMARY KEY AUTOINCREMENT"
            int_type = "INTEGER"
            text_type = "TEXT"
            real_type = "REAL"

        # Table 1: Script Analysis
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS script_analysis (
            id {auto_id},
            video_id TEXT NOT NULL,
            channel_code TEXT,
            analysis_timestamp TEXT NOT NULL,
            script_quality_score {real_type},
            hook_effectiveness_score {real_type},
            call_to_action_score {real_type},
            persuasion_effectiveness_score {real_type},
            user_intent_match_score {real_type},
            persuasion_techniques TEXT,
            key_strengths TEXT,
            improvement_areas TEXT,
            target_audience TEXT,
            content_value_score {real_type},
            identified_intent TEXT,
            has_clear_intro {int_type},
            has_clear_cta {int_type},
            problem_solution_structure {int_type},
            readability_score {real_type},
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Table 2: Affiliate Recommendations
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS affiliate_recommendations (
            id {auto_id},
            video_id TEXT NOT NULL,
            recommendation_timestamp TEXT NOT NULL,
            product_rank {int_type},
            product_name TEXT,
            product_category TEXT,
            relevance_score {real_type},
            conversion_probability {real_type},
            recommendation_reasoning TEXT,
            where_to_mention TEXT,
            mentioned_in_video {int_type},
            amazon_asin TEXT,
            price_range TEXT
        )
        """)

        # Table 3: Description Analysis
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS description_analysis (
            id {auto_id},
            video_id TEXT NOT NULL,
            analysis_timestamp TEXT NOT NULL,
            cta_effectiveness_score {real_type},
            description_quality_score {real_type},
            seo_score {real_type},
            total_links {int_type},
            affiliate_links {int_type},
            link_positioning_score {real_type},
            has_clear_cta {int_type},
            optimization_suggestions TEXT,
            missing_elements TEXT,
            strengths TEXT,
            yt_total_views {int_type} DEFAULT 0,
            yt_total_impressions {int_type} DEFAULT 0,
            yt_overall_ctr {real_type} DEFAULT 0.0,
            yt_by_traffic_source TEXT,
            main_keyword TEXT,
            silo TEXT,
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Table 4: Conversion Analysis
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS conversion_analysis (
            id {auto_id},
            video_id TEXT NOT NULL,
            analysis_timestamp TEXT NOT NULL,
            metrics_date TEXT,
            revenue {real_type},
            clicks {int_type},
            sales {int_type},
            views {int_type},
            conversion_rate {real_type},
            revenue_per_click {real_type},
            revenue_per_1k_views {real_type},
            conversion_drivers TEXT,
            underperformance_reasons TEXT,
            recommendations TEXT,
            UNIQUE(video_id, analysis_timestamp)
        )
        """)

        # Table 5: Video Transcripts
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS video_transcripts (
            id {auto_id},
            video_id TEXT NOT NULL UNIQUE,
            title TEXT,
            channel TEXT,
            duration_seconds {int_type},
            transcript TEXT NOT NULL,
            word_count {int_type},
            provider TEXT,
            segments TEXT,
            frames_json TEXT,
            frame_count {int_type} DEFAULT 0,
            frame_interval_seconds {int_type},
            frame_analysis TEXT,
            emotions TEXT,
            description TEXT,
            content_insights TEXT,
            transcribed_at TEXT NOT NULL,
            updated_at TEXT
        )
        """)

        # Table 6: Video Comments (from YouTube Data API)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS video_comments (
            id {auto_id},
            video_id TEXT NOT NULL,
            comment_id TEXT NOT NULL UNIQUE,
            comment_text TEXT,
            author_name TEXT,
            author_channel_id TEXT,
            like_count {int_type} DEFAULT 0,
            is_pinned {int_type} DEFAULT 0,
            is_channel_owner {int_type} DEFAULT 0,
            published_at TEXT,
            links_found TEXT,
            brands_detected TEXT,
            fetched_at TEXT NOT NULL
        )
        """)

        # Table 7: Transcript History
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS video_transcripts_history (
            id {auto_id},
            video_id TEXT NOT NULL,
            title TEXT,
            channel TEXT,
            duration_seconds {int_type},
            transcript TEXT,
            word_count {int_type},
            provider TEXT,
            segments TEXT,
            frames_json TEXT,
            frame_count {int_type} DEFAULT 0,
            frame_interval_seconds {int_type},
            frame_analysis TEXT,
            emotions TEXT,
            description TEXT,
            content_insights TEXT,
            original_transcribed_at TEXT,
            archived_at TEXT NOT NULL
        )
        """)

        # Table 8: CTA Audit Scores (for conversion audit page)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS cta_audit_scores (
            id {auto_id},
            video_id TEXT NOT NULL UNIQUE,
            cta_score {real_type},
            description_score {real_type},
            base_score {real_type},
            has_preferred_brand {int_type} DEFAULT 0,
            preferred_brand TEXT,
            adjusted_score {real_type},
            scoring_reasoning TEXT,
            scored_at TEXT NOT NULL
        )
        """)

        # Table 9: Approved Brands (silo -> brand mapping for gate checks)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS approved_brands (
            id {auto_id},
            silo TEXT NOT NULL UNIQUE,
            primary_brand TEXT NOT NULL,
            secondary_brand TEXT,
            notes TEXT,
            updated_at TEXT NOT NULL
        )
        """)

        # Table 10: Partner List (brands with active revenue relationships)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS partner_list (
            id {auto_id},
            brand_name TEXT NOT NULL UNIQUE,
            silo TEXT,
            is_active {int_type} DEFAULT 1,
            notes TEXT,
            added_at TEXT NOT NULL,
            updated_at TEXT
        )
        """)

        # Table 11: Script Scores (comprehensive scoring - gates + quality + multiplier)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS script_scores (
            id {auto_id},
            video_id TEXT NOT NULL,
            scored_at TEXT NOT NULL,
            scoring_version TEXT DEFAULT '1.0',

            gates_json TEXT,
            gates_passed {int_type} DEFAULT 0,
            gates_total {int_type} DEFAULT 0,
            all_gates_passed {int_type} DEFAULT 0,

            quality_score_total {real_type},
            specificity_score {real_type},
            conversion_arch_score {real_type},
            retention_arch_score {real_type},
            authenticity_score {real_type},
            viewer_respect_score {real_type},
            production_score {real_type},
            dimension_details_json TEXT,

            keyword_tier TEXT,
            domination_score {real_type},
            context_multiplier {real_type},
            multiplied_score {real_type},
            quality_floor {real_type},
            passes_quality_floor {int_type} DEFAULT 1,

            action_items_json TEXT,

            rizz_score {real_type},
            rizz_vocal_score {real_type},
            rizz_copy_score {real_type},
            rizz_details_json TEXT,

            transcript_length {int_type},
            model_used TEXT,
            prompt_version TEXT,
            cost_estimate {real_type},

            UNIQUE(video_id, scored_at)
        )
        """)

        # Create indexes (syntax is the same for both)
        indexes = [
            ("idx_script_video_id", "script_analysis", "video_id"),
            ("idx_affiliate_video_id", "affiliate_recommendations", "video_id"),
            ("idx_description_video_id", "description_analysis", "video_id"),
            ("idx_conversion_video_id", "conversion_analysis", "video_id"),
            ("idx_transcript_video_id", "video_transcripts", "video_id"),
            ("idx_transcript_history_video_id", "video_transcripts_history", "video_id"),
            ("idx_comments_video_id", "video_comments", "video_id"),
            ("idx_cta_audit_video_id", "cta_audit_scores", "video_id"),
            ("idx_script_scores_video_id", "script_scores", "video_id"),
            ("idx_approved_brands_silo", "approved_brands", "silo"),
            ("idx_partner_list_brand", "partner_list", "brand_name"),
        ]

        for idx_name, table, column in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
            except Exception as e:
                # Index might already exist with different name
                logger.debug(f"Index creation note: {e}")

        conn.commit()
        conn.close()
        logger.info("Database tables initialized successfully")

    def _execute_query(self, query: str, params: tuple = None, fetch: str = None):
        """Execute a query with proper parameter substitution.

        Args:
            query: SQL query with ? placeholders
            params: Query parameters
            fetch: 'one', 'all', or None

        Returns:
            Query results or None
        """
        conn = self._get_connection()

        if self.use_postgres:
            # PostgreSQL uses %s instead of ?
            query = query.replace('?', '%s')
            cursor = conn.cursor()
        else:
            import sqlite3
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if fetch == 'one':
                row = cursor.fetchone()
                if row and self.use_postgres:
                    # Convert PostgreSQL tuple to dict using column names
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, row))
                return dict(row) if row else None
            elif fetch == 'all':
                rows = cursor.fetchall()
                if rows and self.use_postgres:
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in rows]
                return [dict(row) for row in rows] if rows else []
            else:
                conn.commit()
                return cursor.rowcount
        finally:
            conn.close()

    def store_script_analysis(self, analysis: ScriptAnalysis) -> bool:
        """Store script analysis results to local database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            # For PostgreSQL, use ON CONFLICT; for SQLite, use OR REPLACE
            if self.use_postgres:
                query = f"""
                INSERT INTO script_analysis (
                    video_id, channel_code, analysis_timestamp,
                    script_quality_score, hook_effectiveness_score, call_to_action_score,
                    persuasion_effectiveness_score, user_intent_match_score,
                    persuasion_techniques, key_strengths, improvement_areas,
                    target_audience, content_value_score, identified_intent,
                    has_clear_intro, has_clear_cta, problem_solution_structure,
                    readability_score
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                ON CONFLICT (video_id, analysis_timestamp) DO UPDATE SET
                    channel_code = EXCLUDED.channel_code,
                    script_quality_score = EXCLUDED.script_quality_score,
                    hook_effectiveness_score = EXCLUDED.hook_effectiveness_score,
                    call_to_action_score = EXCLUDED.call_to_action_score,
                    persuasion_effectiveness_score = EXCLUDED.persuasion_effectiveness_score,
                    user_intent_match_score = EXCLUDED.user_intent_match_score,
                    persuasion_techniques = EXCLUDED.persuasion_techniques,
                    key_strengths = EXCLUDED.key_strengths,
                    improvement_areas = EXCLUDED.improvement_areas,
                    target_audience = EXCLUDED.target_audience,
                    content_value_score = EXCLUDED.content_value_score,
                    identified_intent = EXCLUDED.identified_intent,
                    has_clear_intro = EXCLUDED.has_clear_intro,
                    has_clear_cta = EXCLUDED.has_clear_cta,
                    problem_solution_structure = EXCLUDED.problem_solution_structure,
                    readability_score = EXCLUDED.readability_score
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO script_analysis (
                    video_id, channel_code, analysis_timestamp,
                    script_quality_score, hook_effectiveness_score, call_to_action_score,
                    persuasion_effectiveness_score, user_intent_match_score,
                    persuasion_techniques, key_strengths, improvement_areas,
                    target_audience, content_value_score, identified_intent,
                    has_clear_intro, has_clear_cta, problem_solution_structure,
                    readability_score
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (
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
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            # Delete old recommendations for this video
            if recommendations:
                cursor.execute(
                    f"DELETE FROM affiliate_recommendations WHERE video_id = {p}",
                    (recommendations[0].video_id,)
                )

            # Insert new recommendations
            for rec in recommendations:
                cursor.execute(f"""
                INSERT INTO affiliate_recommendations (
                    video_id, recommendation_timestamp, product_rank,
                    product_name, product_category, relevance_score,
                    conversion_probability, recommendation_reasoning,
                    where_to_mention, mentioned_in_video, amazon_asin, price_range
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
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
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            if self.use_postgres:
                query = f"""
                INSERT INTO description_analysis (
                    video_id, analysis_timestamp, cta_effectiveness_score,
                    description_quality_score, seo_score, total_links,
                    affiliate_links, link_positioning_score, has_clear_cta,
                    optimization_suggestions, missing_elements, strengths,
                    yt_total_views, yt_total_impressions, yt_overall_ctr,
                    yt_by_traffic_source, main_keyword, silo
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                ON CONFLICT (video_id, analysis_timestamp) DO UPDATE SET
                    cta_effectiveness_score = EXCLUDED.cta_effectiveness_score,
                    description_quality_score = EXCLUDED.description_quality_score,
                    seo_score = EXCLUDED.seo_score,
                    total_links = EXCLUDED.total_links,
                    affiliate_links = EXCLUDED.affiliate_links,
                    link_positioning_score = EXCLUDED.link_positioning_score,
                    has_clear_cta = EXCLUDED.has_clear_cta,
                    optimization_suggestions = EXCLUDED.optimization_suggestions,
                    missing_elements = EXCLUDED.missing_elements,
                    strengths = EXCLUDED.strengths,
                    yt_total_views = EXCLUDED.yt_total_views,
                    yt_total_impressions = EXCLUDED.yt_total_impressions,
                    yt_overall_ctr = EXCLUDED.yt_overall_ctr,
                    yt_by_traffic_source = EXCLUDED.yt_by_traffic_source,
                    main_keyword = EXCLUDED.main_keyword,
                    silo = EXCLUDED.silo
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO description_analysis (
                    video_id, analysis_timestamp, cta_effectiveness_score,
                    description_quality_score, seo_score, total_links,
                    affiliate_links, link_positioning_score, has_clear_cta,
                    optimization_suggestions, missing_elements, strengths,
                    yt_total_views, yt_total_impressions, yt_overall_ctr,
                    yt_by_traffic_source, main_keyword, silo
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (
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
                json.dumps(analysis.strengths),
                analysis.yt_total_views,
                analysis.yt_total_impressions,
                analysis.yt_overall_ctr,
                json.dumps(analysis.yt_by_traffic_source),
                analysis.main_keyword,
                analysis.silo
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
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            if self.use_postgres:
                query = f"""
                INSERT INTO conversion_analysis (
                    video_id, analysis_timestamp, metrics_date,
                    revenue, clicks, sales, views,
                    conversion_rate, revenue_per_click, revenue_per_1k_views,
                    conversion_drivers, underperformance_reasons, recommendations
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                ON CONFLICT (video_id, analysis_timestamp) DO UPDATE SET
                    metrics_date = EXCLUDED.metrics_date,
                    revenue = EXCLUDED.revenue,
                    clicks = EXCLUDED.clicks,
                    sales = EXCLUDED.sales,
                    views = EXCLUDED.views,
                    conversion_rate = EXCLUDED.conversion_rate,
                    revenue_per_click = EXCLUDED.revenue_per_click,
                    revenue_per_1k_views = EXCLUDED.revenue_per_1k_views,
                    conversion_drivers = EXCLUDED.conversion_drivers,
                    underperformance_reasons = EXCLUDED.underperformance_reasons,
                    recommendations = EXCLUDED.recommendations
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO conversion_analysis (
                    video_id, analysis_timestamp, metrics_date,
                    revenue, clicks, sales, views,
                    conversion_rate, revenue_per_click, revenue_per_1k_views,
                    conversion_drivers, underperformance_reasons, recommendations
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (
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
            row = self._execute_query("""
            SELECT * FROM script_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,), fetch='one')

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
            rows = self._execute_query("""
            SELECT * FROM affiliate_recommendations
            WHERE video_id = ?
            ORDER BY recommendation_timestamp DESC, product_rank ASC
            LIMIT 5
            """, (video_id,), fetch='all')

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
            row = self._execute_query("""
            SELECT * FROM description_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,), fetch='one')

            if row:
                # Handle new YT Analytics columns that may not exist in older records
                yt_total_views = row.get('yt_total_views', 0) or 0
                yt_total_impressions = row.get('yt_total_impressions', 0) or 0
                yt_overall_ctr = row.get('yt_overall_ctr', 0.0) or 0.0
                yt_by_traffic_source = json.loads(row['yt_by_traffic_source']) if row.get('yt_by_traffic_source') else []
                main_keyword = row.get('main_keyword', '') or ''
                silo = row.get('silo', '') or ''

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
                    strengths=json.loads(row['strengths']) if row['strengths'] else [],
                    yt_total_views=yt_total_views,
                    yt_total_impressions=yt_total_impressions,
                    yt_overall_ctr=yt_overall_ctr,
                    yt_by_traffic_source=yt_by_traffic_source,
                    main_keyword=main_keyword,
                    silo=silo
                )
            return None

        except Exception as e:
            logger.error(f"Error fetching description analysis: {str(e)}")
            return None

    def get_conversion_analysis(self, video_id: str) -> Optional[ConversionAnalysis]:
        """Fetch latest conversion analysis for a video."""
        try:
            row = self._execute_query("""
            SELECT * FROM conversion_analysis
            WHERE video_id = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 1
            """, (video_id,), fetch='one')

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
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            cursor.execute(f"""
            SELECT 1 FROM script_analysis WHERE video_id = {p}
            UNION
            SELECT 1 FROM description_analysis WHERE video_id = {p}
            UNION
            SELECT 1 FROM conversion_analysis WHERE video_id = {p}
            LIMIT 1
            """, (video_id, video_id, video_id))

            result = cursor.fetchone()
            conn.close()

            return result is not None

        except Exception as e:
            logger.error(f"Error checking analysis: {str(e)}")
            return False

    # ==================== TRANSCRIPT METHODS ====================

    def store_transcript(self, video_id: str, title: str, channel: str,
                        duration_seconds: int, transcript: str, word_count: int,
                        provider: str, segments: list = None, frames: list = None,
                        frame_interval: int = None, frame_analysis: list = None,
                        emotions: dict = None, description: str = None,
                        content_insights: dict = None) -> bool:
        """Store video transcript, frame analysis, emotion data, description, and content insights."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            now = datetime.now().isoformat()

            # Check if data already exists and archive it
            cursor.execute(f"SELECT * FROM video_transcripts WHERE video_id = {p}", (video_id,))
            existing = cursor.fetchone()

            if existing:
                # Get column names for existing row
                if self.use_postgres:
                    columns = [desc[0] for desc in cursor.description]
                    existing = dict(zip(columns, existing))
                else:
                    import sqlite3
                    existing = dict(existing) if hasattr(existing, 'keys') else None
                    if not existing:
                        # Re-fetch with row factory
                        conn.close()
                        conn = self._get_connection()
                        if not self.use_postgres:
                            import sqlite3
                            conn.row_factory = sqlite3.Row
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT * FROM video_transcripts WHERE video_id = {p}", (video_id,))
                        existing = cursor.fetchone()
                        if existing:
                            existing = dict(existing)

            if existing:
                # Archive old data to history table
                logger.info(f"Archiving existing transcript data for video {video_id}")
                cursor.execute(f"""
                INSERT INTO video_transcripts_history (
                    video_id, title, channel, duration_seconds, transcript,
                    word_count, provider, segments, frames_json, frame_count,
                    frame_interval_seconds, frame_analysis, emotions, description,
                    content_insights, original_transcribed_at, archived_at
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """, (
                    existing.get('video_id'),
                    existing.get('title'),
                    existing.get('channel'),
                    existing.get('duration_seconds'),
                    existing.get('transcript'),
                    existing.get('word_count'),
                    existing.get('provider'),
                    existing.get('segments'),
                    existing.get('frames_json'),
                    existing.get('frame_count'),
                    existing.get('frame_interval_seconds'),
                    existing.get('frame_analysis'),
                    existing.get('emotions'),
                    existing.get('description'),
                    existing.get('content_insights'),
                    existing.get('transcribed_at'),
                    now
                ))
                logger.info(f"Archived transcript to history for video {video_id}")

            # Delete existing record if any (for clean insert)
            cursor.execute(f"DELETE FROM video_transcripts WHERE video_id = {p}", (video_id,))

            # Insert new data
            cursor.execute(f"""
            INSERT INTO video_transcripts (
                video_id, title, channel, duration_seconds, transcript,
                word_count, provider, segments, frames_json, frame_count,
                frame_interval_seconds, frame_analysis, emotions, description,
                content_insights, transcribed_at, updated_at
            ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
            """, (
                video_id,
                title,
                channel,
                duration_seconds,
                transcript,
                word_count,
                provider,
                json.dumps(segments) if segments else None,
                json.dumps(frames) if frames else None,
                len(frames) if frames else 0,
                frame_interval,
                json.dumps(frame_analysis) if frame_analysis else None,
                json.dumps(emotions) if emotions else None,
                description,
                json.dumps(content_insights) if content_insights else None,
                now,
                now
            ))

            conn.commit()
            conn.close()
            logger.info(f"Stored transcript for video {video_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing transcript: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def get_transcript(self, video_id: str) -> Optional[dict]:
        """Get transcript for a video."""
        try:
            row = self._execute_query("""
            SELECT * FROM video_transcripts WHERE video_id = ?
            """, (video_id,), fetch='one')

            if row:
                result = {
                    'video_id': row['video_id'],
                    'title': row['title'],
                    'channel': row['channel'],
                    'duration_seconds': row['duration_seconds'],
                    'transcript': row['transcript'],
                    'word_count': row['word_count'],
                    'provider': row['provider'],
                    'segments': json.loads(row['segments']) if row.get('segments') else None,
                    'frames': json.loads(row['frames_json']) if row.get('frames_json') else None,
                    'frame_count': row.get('frame_count', 0),
                    'frame_interval_seconds': row.get('frame_interval_seconds'),
                    'transcribed_at': row['transcribed_at'],
                    'updated_at': row.get('updated_at'),
                    'frame_analysis': json.loads(row['frame_analysis']) if row.get('frame_analysis') else None,
                    'emotions': json.loads(row['emotions']) if row.get('emotions') else None,
                    'description': row.get('description'),
                    'content_insights': json.loads(row['content_insights']) if row.get('content_insights') else None
                }
                return result
            return None

        except Exception as e:
            logger.error(f"Error fetching transcript: {str(e)}")
            return None

    def has_transcript(self, video_id: str) -> bool:
        """Check if a video has a stored transcript."""
        try:
            row = self._execute_query(
                "SELECT 1 FROM video_transcripts WHERE video_id = ?",
                (video_id,),
                fetch='one'
            )
            return row is not None
        except Exception as e:
            logger.error(f"Error checking transcript: {str(e)}")
            return False

    def delete_transcript(self, video_id: str) -> bool:
        """Delete transcript for a video (for re-transcription)."""
        try:
            self._execute_query(
                "DELETE FROM video_transcripts WHERE video_id = ?",
                (video_id,)
            )
            logger.info(f"Deleted transcript for video {video_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting transcript: {str(e)}")
            return False

    def get_all_transcripts(self, limit: int = 50) -> list:
        """Get all stored transcripts (for history view)."""
        try:
            rows = self._execute_query(f"""
            SELECT video_id, title, channel, duration_seconds, word_count,
                   provider, frame_count, transcribed_at
            FROM video_transcripts
            ORDER BY transcribed_at DESC
            LIMIT ?
            """, (limit,), fetch='all')

            return rows if rows else []

        except Exception as e:
            logger.error(f"Error fetching transcripts: {str(e)}")
            return []

    def get_transcript_history(self, video_id: str, limit: int = 10) -> List[dict]:
        """Get historical transcript data for a video."""
        try:
            rows = self._execute_query(f"""
            SELECT id, video_id, title, channel, duration_seconds, word_count,
                   provider, frame_count, original_transcribed_at, archived_at,
                   CASE WHEN transcript IS NOT NULL THEN 1 ELSE 0 END as has_transcript,
                   CASE WHEN emotions IS NOT NULL THEN 1 ELSE 0 END as has_emotions,
                   CASE WHEN frame_analysis IS NOT NULL THEN 1 ELSE 0 END as has_frames,
                   CASE WHEN content_insights IS NOT NULL THEN 1 ELSE 0 END as has_insights
            FROM video_transcripts_history
            WHERE video_id = ?
            ORDER BY archived_at DESC
            LIMIT ?
            """, (video_id, limit), fetch='all')

            return rows if rows else []

        except Exception as e:
            logger.error(f"Error fetching transcript history: {str(e)}")
            return []

    def get_transcript_history_detail(self, history_id: int) -> Optional[dict]:
        """Get full details of a historical transcript entry."""
        try:
            row = self._execute_query("""
            SELECT * FROM video_transcripts_history WHERE id = ?
            """, (history_id,), fetch='one')

            if row:
                result = dict(row)
                # Parse JSON fields
                if result.get('segments'):
                    result['segments'] = json.loads(result['segments'])
                if result.get('frames_json'):
                    result['frames'] = json.loads(result['frames_json'])
                if result.get('frame_analysis'):
                    result['frame_analysis'] = json.loads(result['frame_analysis'])
                if result.get('emotions'):
                    result['emotions'] = json.loads(result['emotions'])
                if result.get('content_insights'):
                    result['content_insights'] = json.loads(result['content_insights'])
                return result
            return None

        except Exception as e:
            logger.error(f"Error fetching transcript history detail: {str(e)}")
            return None

    def update_content_insights(self, video_id: str, content_insights: dict) -> bool:
        """Update only the content_insights field for an existing transcript."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            now = datetime.now().isoformat()

            cursor.execute(f"""
            UPDATE video_transcripts
            SET content_insights = {p}, updated_at = {p}
            WHERE video_id = {p}
            """, (
                json.dumps(content_insights) if content_insights else None,
                now,
                video_id
            ))

            rowcount = cursor.rowcount
            conn.commit()
            conn.close()

            if rowcount == 0:
                logger.warning(f"No transcript found to update for video {video_id}")
                return False

            logger.info(f"Updated content_insights for video {video_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating content_insights: {str(e)}")
            return False

    # ==================== VIDEO COMMENTS METHODS ====================

    def store_comments(self, comments: list) -> int:
        """Store video comments fetched from YouTube API.

        Args:
            comments: List of dicts with keys: video_id, comment_id, comment_text,
                      author_name, author_channel_id, like_count, is_pinned,
                      is_channel_owner, published_at, links_found, brands_detected

        Returns:
            Number of comments stored
        """
        if not comments:
            return 0

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()
            now = datetime.now().isoformat()
            stored = 0

            for c in comments:
                if self.use_postgres:
                    query = f"""
                    INSERT INTO video_comments (
                        video_id, comment_id, comment_text, author_name,
                        author_channel_id, like_count, is_pinned, is_channel_owner,
                        published_at, links_found, brands_detected, fetched_at
                    ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                    ON CONFLICT (comment_id) DO UPDATE SET
                        comment_text = EXCLUDED.comment_text,
                        like_count = EXCLUDED.like_count,
                        is_pinned = EXCLUDED.is_pinned,
                        links_found = EXCLUDED.links_found,
                        brands_detected = EXCLUDED.brands_detected,
                        fetched_at = EXCLUDED.fetched_at
                    """
                else:
                    query = f"""
                    INSERT OR REPLACE INTO video_comments (
                        video_id, comment_id, comment_text, author_name,
                        author_channel_id, like_count, is_pinned, is_channel_owner,
                        published_at, links_found, brands_detected, fetched_at
                    ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                    """

                cursor.execute(query, (
                    c['video_id'],
                    c['comment_id'],
                    c.get('comment_text', ''),
                    c.get('author_name', ''),
                    c.get('author_channel_id', ''),
                    c.get('like_count', 0),
                    1 if c.get('is_pinned') else 0,
                    1 if c.get('is_channel_owner') else 0,
                    c.get('published_at', ''),
                    json.dumps(c.get('links_found', [])),
                    json.dumps(c.get('brands_detected', [])),
                    now
                ))
                stored += 1

            conn.commit()
            conn.close()
            logger.info(f"Stored {stored} comments for video {comments[0]['video_id']}")
            return stored

        except Exception as e:
            logger.error(f"Error storing comments: {str(e)}")
            return 0

    def get_comments(self, video_id: str) -> list:
        """Get all stored comments for a video."""
        try:
            rows = self._execute_query("""
            SELECT * FROM video_comments
            WHERE video_id = ?
            ORDER BY is_pinned DESC, like_count DESC
            """, (video_id,), fetch='all')

            results = []
            for row in (rows or []):
                r = dict(row)
                r['links_found'] = json.loads(r['links_found']) if r.get('links_found') else []
                r['brands_detected'] = json.loads(r['brands_detected']) if r.get('brands_detected') else []
                r['is_pinned'] = bool(r.get('is_pinned'))
                r['is_channel_owner'] = bool(r.get('is_channel_owner'))
                results.append(r)
            return results

        except Exception as e:
            logger.error(f"Error fetching comments: {str(e)}")
            return []

    def get_pinned_comment(self, video_id: str) -> Optional[dict]:
        """Get the pinned comment for a video (if any)."""
        try:
            row = self._execute_query("""
            SELECT * FROM video_comments
            WHERE video_id = ? AND is_pinned = 1
            LIMIT 1
            """, (video_id,), fetch='one')

            if row:
                r = dict(row)
                r['links_found'] = json.loads(r['links_found']) if r.get('links_found') else []
                r['brands_detected'] = json.loads(r['brands_detected']) if r.get('brands_detected') else []
                r['is_pinned'] = bool(r.get('is_pinned'))
                r['is_channel_owner'] = bool(r.get('is_channel_owner'))
                return r
            return None

        except Exception as e:
            logger.error(f"Error fetching pinned comment: {str(e)}")
            return None

    def get_comments_summary(self, video_ids: list = None) -> dict:
        """Get comment summary (pinned comment brand, text, links) for multiple videos.

        Returns:
            Dict mapping video_id -> {pinned_brand, pinned_text, pinned_links, has_comments}
        """
        try:
            if video_ids:
                placeholders = ','.join(['?' for _ in video_ids])
                rows = self._execute_query(f"""
                SELECT video_id, comment_text, brands_detected, links_found,
                       is_pinned, is_channel_owner, author_name
                FROM video_comments
                WHERE video_id IN ({placeholders}) AND is_pinned = 1
                """, tuple(video_ids), fetch='all')
            else:
                rows = self._execute_query("""
                SELECT video_id, comment_text, brands_detected, links_found,
                       is_pinned, is_channel_owner, author_name
                FROM video_comments
                WHERE is_pinned = 1
                """, fetch='all')

            summary = {}
            for row in (rows or []):
                vid = row['video_id']
                brands = json.loads(row['brands_detected']) if row.get('brands_detected') else []
                links = json.loads(row['links_found']) if row.get('links_found') else []
                summary[vid] = {
                    'pinned_brand': brands[0] if brands else None,
                    'pinned_text': row.get('comment_text', ''),
                    'pinned_author': row.get('author_name', ''),
                    'pinned_links': links,
                    'has_comments': True
                }
            return summary

        except Exception as e:
            logger.error(f"Error fetching comments summary: {str(e)}")
            return {}

    def has_comments(self, video_id: str) -> bool:
        """Check if comments have been fetched for a video."""
        try:
            row = self._execute_query(
                "SELECT 1 FROM video_comments WHERE video_id = ? LIMIT 1",
                (video_id,),
                fetch='one'
            )
            return row is not None
        except Exception as e:
            logger.error(f"Error checking comments: {str(e)}")
            return False

    def delete_comments(self, video_id: str) -> bool:
        """Delete all comments for a video (for re-fetching)."""
        try:
            self._execute_query(
                "DELETE FROM video_comments WHERE video_id = ?",
                (video_id,)
            )
            logger.info(f"Deleted comments for video {video_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting comments: {str(e)}")
            return False

    # ==================== CTA AUDIT SCORE METHODS ====================

    def store_cta_audit_score(self, video_id: str, cta_score: float,
                              description_score: float, base_score: float,
                              has_preferred_brand: bool, preferred_brand: str,
                              adjusted_score: float, scoring_reasoning: str) -> bool:
        """Store or update CTA audit score for a video."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()
            now = datetime.now().isoformat()

            if self.use_postgres:
                query = f"""
                INSERT INTO cta_audit_scores (
                    video_id, cta_score, description_score, base_score,
                    has_preferred_brand, preferred_brand, adjusted_score,
                    scoring_reasoning, scored_at
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                ON CONFLICT (video_id) DO UPDATE SET
                    cta_score = EXCLUDED.cta_score,
                    description_score = EXCLUDED.description_score,
                    base_score = EXCLUDED.base_score,
                    has_preferred_brand = EXCLUDED.has_preferred_brand,
                    preferred_brand = EXCLUDED.preferred_brand,
                    adjusted_score = EXCLUDED.adjusted_score,
                    scoring_reasoning = EXCLUDED.scoring_reasoning,
                    scored_at = EXCLUDED.scored_at
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO cta_audit_scores (
                    video_id, cta_score, description_score, base_score,
                    has_preferred_brand, preferred_brand, adjusted_score,
                    scoring_reasoning, scored_at
                ) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (
                video_id, cta_score, description_score, base_score,
                1 if has_preferred_brand else 0, preferred_brand or '',
                adjusted_score, scoring_reasoning or '', now
            ))

            conn.commit()
            conn.close()
            logger.info(f"Stored CTA audit score for video {video_id}: {adjusted_score}")
            return True

        except Exception as e:
            logger.error(f"Error storing CTA audit score: {str(e)}")
            return False

    def get_cta_audit_scores(self, video_ids: list) -> dict:
        """Get CTA audit scores for multiple videos.

        Returns:
            Dict mapping video_id -> {cta_score, description_score, base_score,
                                      has_preferred_brand, preferred_brand,
                                      adjusted_score, scoring_reasoning, scored_at}
        """
        if not video_ids:
            return {}
        try:
            placeholders = ','.join(['?' for _ in video_ids])
            rows = self._execute_query(f"""
            SELECT * FROM cta_audit_scores
            WHERE video_id IN ({placeholders})
            """, tuple(video_ids), fetch='all')

            result = {}
            for row in (rows or []):
                result[row['video_id']] = {
                    'cta_score': row['cta_score'],
                    'description_score': row['description_score'],
                    'base_score': row['base_score'],
                    'has_preferred_brand': bool(row['has_preferred_brand']),
                    'preferred_brand': row['preferred_brand'],
                    'adjusted_score': row['adjusted_score'],
                    'scoring_reasoning': row['scoring_reasoning'],
                    'scored_at': row['scored_at'],
                }
            return result

        except Exception as e:
            logger.error(f"Error fetching CTA audit scores: {str(e)}")
            return {}

    # ========== Script Scores (Gates + Quality + Multiplier) ==========

    def store_script_score(self, score: ScriptScore) -> bool:
        """Store or update script score for a video."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()

            gates_json = json.dumps([
                {'gate_name': g.gate_name, 'passed': g.passed, 'failure_reason': g.failure_reason}
                for g in score.gate_results
            ]) if score.gate_results else None
            gates_passed = sum(1 for g in score.gate_results if g.passed) if score.gate_results else 0
            gates_total = len(score.gate_results) if score.gate_results else 0
            dimension_details_json = json.dumps(score.dimension_details) if score.dimension_details else None
            action_items_json = json.dumps(score.action_items) if score.action_items else None
            rizz_details_json = json.dumps(score.rizz_details) if score.rizz_details else None

            if self.use_postgres:
                query = f"""
                INSERT INTO script_scores (
                    video_id, scored_at, scoring_version,
                    gates_json, gates_passed, gates_total, all_gates_passed,
                    quality_score_total, specificity_score, conversion_arch_score,
                    retention_arch_score, authenticity_score, viewer_respect_score,
                    production_score, dimension_details_json,
                    keyword_tier, domination_score, context_multiplier,
                    multiplied_score, quality_floor, passes_quality_floor,
                    action_items_json,
                    rizz_score, rizz_vocal_score, rizz_copy_score, rizz_details_json
                ) VALUES ({','.join([p]*26)})
                ON CONFLICT (video_id, scored_at) DO UPDATE SET
                    scoring_version = EXCLUDED.scoring_version,
                    gates_json = EXCLUDED.gates_json,
                    gates_passed = EXCLUDED.gates_passed,
                    gates_total = EXCLUDED.gates_total,
                    all_gates_passed = EXCLUDED.all_gates_passed,
                    quality_score_total = EXCLUDED.quality_score_total,
                    specificity_score = EXCLUDED.specificity_score,
                    conversion_arch_score = EXCLUDED.conversion_arch_score,
                    retention_arch_score = EXCLUDED.retention_arch_score,
                    authenticity_score = EXCLUDED.authenticity_score,
                    viewer_respect_score = EXCLUDED.viewer_respect_score,
                    production_score = EXCLUDED.production_score,
                    dimension_details_json = EXCLUDED.dimension_details_json,
                    keyword_tier = EXCLUDED.keyword_tier,
                    domination_score = EXCLUDED.domination_score,
                    context_multiplier = EXCLUDED.context_multiplier,
                    multiplied_score = EXCLUDED.multiplied_score,
                    quality_floor = EXCLUDED.quality_floor,
                    passes_quality_floor = EXCLUDED.passes_quality_floor,
                    action_items_json = EXCLUDED.action_items_json,
                    rizz_score = EXCLUDED.rizz_score,
                    rizz_vocal_score = EXCLUDED.rizz_vocal_score,
                    rizz_copy_score = EXCLUDED.rizz_copy_score,
                    rizz_details_json = EXCLUDED.rizz_details_json
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO script_scores (
                    video_id, scored_at, scoring_version,
                    gates_json, gates_passed, gates_total, all_gates_passed,
                    quality_score_total, specificity_score, conversion_arch_score,
                    retention_arch_score, authenticity_score, viewer_respect_score,
                    production_score, dimension_details_json,
                    keyword_tier, domination_score, context_multiplier,
                    multiplied_score, quality_floor, passes_quality_floor,
                    action_items_json,
                    rizz_score, rizz_vocal_score, rizz_copy_score, rizz_details_json
                ) VALUES ({','.join([p]*26)})
                """

            cursor.execute(query, (
                score.video_id, score.scored_at.isoformat(), score.scoring_version,
                gates_json, gates_passed, gates_total,
                1 if score.all_gates_passed else 0,
                score.quality_score_total, score.specificity_score,
                score.conversion_arch_score, score.retention_arch_score,
                score.authenticity_score, score.viewer_respect_score,
                score.production_score, dimension_details_json,
                score.keyword_tier, score.domination_score,
                score.context_multiplier, score.multiplied_score,
                score.quality_floor, 1 if score.passes_quality_floor else 0,
                action_items_json,
                score.rizz_score, score.rizz_vocal_score, score.rizz_copy_score,
                rizz_details_json
            ))

            conn.commit()
            conn.close()
            logger.info(f"Stored script score for video {score.video_id}: "
                        f"quality={score.quality_score_total}, gates={gates_passed}/{gates_total}")
            return True

        except Exception as e:
            logger.error(f"Error storing script score: {str(e)}")
            return False

    def get_script_score(self, video_id: str) -> Optional[dict]:
        """Get the latest script score for a video."""
        try:
            row = self._execute_query("""
            SELECT * FROM script_scores
            WHERE video_id = ?
            ORDER BY scored_at DESC LIMIT 1
            """, (video_id,), fetch='one')

            if not row:
                return None

            result = dict(row)
            # Parse JSON fields
            if result.get('gates_json'):
                result['gates'] = json.loads(result['gates_json'])
            else:
                result['gates'] = []
            if result.get('dimension_details_json'):
                result['dimension_details'] = json.loads(result['dimension_details_json'])
            else:
                result['dimension_details'] = {}
            if result.get('action_items_json'):
                result['action_items'] = json.loads(result['action_items_json'])
            else:
                result['action_items'] = []

            if result.get('rizz_details_json'):
                result['rizz_details'] = json.loads(result['rizz_details_json'])
            else:
                result['rizz_details'] = {}
            result['all_gates_passed'] = bool(result.get('all_gates_passed', 0))
            result['passes_quality_floor'] = bool(result.get('passes_quality_floor', 1))
            return result

        except Exception as e:
            logger.error(f"Error fetching script score for {video_id}: {str(e)}")
            return None

    def get_all_script_scores(self) -> list:
        """Get latest script scores for all scored videos (for library view)."""
        try:
            rows = self._execute_query("""
            SELECT s.* FROM script_scores s
            INNER JOIN (
                SELECT video_id, MAX(scored_at) as max_scored
                FROM script_scores GROUP BY video_id
            ) latest ON s.video_id = latest.video_id AND s.scored_at = latest.max_scored
            ORDER BY s.multiplied_score DESC NULLS LAST
            """, fetch='all')

            results = []
            for row in (rows or []):
                r = dict(row)
                if r.get('gates_json'):
                    r['gates'] = json.loads(r['gates_json'])
                if r.get('action_items_json'):
                    r['action_items'] = json.loads(r['action_items_json'])
                r['all_gates_passed'] = bool(r.get('all_gates_passed', 0))
                r['passes_quality_floor'] = bool(r.get('passes_quality_floor', 1))
                results.append(r)
            return results

        except Exception as e:
            logger.error(f"Error fetching all script scores: {str(e)}")
            return []

    def get_scores_by_month(self) -> list:
        """Get all script scores with timestamps for trend view (not just latest per video)."""
        try:
            rows = self._execute_query("""
            SELECT video_id, scored_at, quality_score_total, multiplied_score,
                   rizz_score, all_gates_passed
            FROM script_scores
            WHERE quality_score_total IS NOT NULL
            ORDER BY scored_at ASC
            """, fetch='all')

            results = []
            for row in (rows or []):
                r = dict(row)
                r['all_gates_passed'] = bool(r.get('all_gates_passed', 0))
                results.append(r)
            return results

        except Exception as e:
            logger.error(f"Error fetching scores by month: {str(e)}")
            return []

    # ========== Approved Brands ==========

    def get_approved_brands(self) -> list:
        """Get all approved brand mappings."""
        try:
            rows = self._execute_query(
                "SELECT * FROM approved_brands ORDER BY silo",
                fetch='all'
            )
            return [dict(r) for r in (rows or [])]
        except Exception as e:
            logger.error(f"Error fetching approved brands: {str(e)}")
            return []

    def get_approved_brand_for_silo(self, silo: str) -> Optional[dict]:
        """Get approved brand for a specific silo."""
        try:
            row = self._execute_query(
                "SELECT * FROM approved_brands WHERE LOWER(silo) = LOWER(?)",
                (silo,), fetch='one'
            )
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching approved brand for silo {silo}: {str(e)}")
            return None

    def store_approved_brand(self, silo: str, primary_brand: str,
                             secondary_brand: str = None, notes: str = None) -> bool:
        """Store or update approved brand for a silo."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()
            now = datetime.now().isoformat()

            if self.use_postgres:
                query = f"""
                INSERT INTO approved_brands (silo, primary_brand, secondary_brand, notes, updated_at)
                VALUES ({p}, {p}, {p}, {p}, {p})
                ON CONFLICT (silo) DO UPDATE SET
                    primary_brand = EXCLUDED.primary_brand,
                    secondary_brand = EXCLUDED.secondary_brand,
                    notes = EXCLUDED.notes,
                    updated_at = EXCLUDED.updated_at
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO approved_brands (silo, primary_brand, secondary_brand, notes, updated_at)
                VALUES ({p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (silo, primary_brand, secondary_brand, notes, now))
            conn.commit()
            conn.close()
            logger.info(f"Stored approved brand: {silo} -> {primary_brand}")
            return True
        except Exception as e:
            logger.error(f"Error storing approved brand: {str(e)}")
            return False

    def delete_approved_brand(self, silo: str) -> bool:
        """Delete approved brand for a silo."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM approved_brands WHERE LOWER(silo) = LOWER(?)"
                           .replace('?', '%s') if self.use_postgres else
                           "DELETE FROM approved_brands WHERE LOWER(silo) = LOWER(?)",
                           (silo,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error deleting approved brand: {str(e)}")
            return False

    # ========== Partner List ==========

    def get_partner_list(self, active_only: bool = True) -> list:
        """Get partner brands."""
        try:
            query = "SELECT * FROM partner_list"
            if active_only:
                query += " WHERE is_active = 1"
            query += " ORDER BY brand_name"
            rows = self._execute_query(query, fetch='all')
            return [dict(r) for r in (rows or [])]
        except Exception as e:
            logger.error(f"Error fetching partner list: {str(e)}")
            return []

    def store_partner(self, brand_name: str, silo: str = None,
                      is_active: bool = True, notes: str = None) -> bool:
        """Store or update a partner brand."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            p = self._get_placeholder()
            now = datetime.now().isoformat()

            if self.use_postgres:
                query = f"""
                INSERT INTO partner_list (brand_name, silo, is_active, notes, added_at, updated_at)
                VALUES ({p}, {p}, {p}, {p}, {p}, {p})
                ON CONFLICT (brand_name) DO UPDATE SET
                    silo = EXCLUDED.silo,
                    is_active = EXCLUDED.is_active,
                    notes = EXCLUDED.notes,
                    updated_at = EXCLUDED.updated_at
                """
            else:
                query = f"""
                INSERT OR REPLACE INTO partner_list (brand_name, silo, is_active, notes, added_at, updated_at)
                VALUES ({p}, {p}, {p}, {p}, {p}, {p})
                """

            cursor.execute(query, (brand_name, silo, 1 if is_active else 0, notes, now, now))
            conn.commit()
            conn.close()
            logger.info(f"Stored partner: {brand_name}")
            return True
        except Exception as e:
            logger.error(f"Error storing partner: {str(e)}")
            return False

    def delete_partner(self, brand_name: str) -> bool:
        """Delete a partner brand."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM partner_list WHERE LOWER(brand_name) = LOWER(?)"
                           .replace('?', '%s') if self.use_postgres else
                           "DELETE FROM partner_list WHERE LOWER(brand_name) = LOWER(?)",
                           (brand_name,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error deleting partner: {str(e)}")
            return False
