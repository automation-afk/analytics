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
            # Check for environment variable first (for Railway/production)
            db_path = os.getenv('DATABASE_PATH')

            if db_path is None:
                # Default to data/analysis.db in app directory
                # Use /app/data for Railway, local path for development
                if os.path.exists('/app'):
                    # Running on Railway/Docker
                    data_dir = '/app/data'
                else:
                    # Local development
                    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                    data_dir = os.path.join(base_dir, 'data')

                os.makedirs(data_dir, exist_ok=True)
                db_path = os.path.join(data_dir, 'analysis.db')
                logger.info(f"Using database path: {db_path}")

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

        # Table 5: Video Transcripts (for AI analysis)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL UNIQUE,
            title TEXT,
            channel TEXT,
            duration_seconds INTEGER,
            transcript TEXT NOT NULL,
            word_count INTEGER,
            provider TEXT,  -- groq or openai
            segments TEXT,  -- JSON array with timestamps
            frames_json TEXT,  -- JSON array of frame timestamps
            frame_count INTEGER DEFAULT 0,
            frame_interval_seconds INTEGER,
            frame_analysis TEXT,  -- JSON: vision AI analysis of frames (text, no images)
            emotions TEXT,  -- JSON: Hume AI voice emotion analysis
            transcribed_at TEXT NOT NULL,
            updated_at TEXT
        )
        """)

        # Table 6: Transcript History (stores old versions when data is replaced)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_transcripts_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            title TEXT,
            channel TEXT,
            duration_seconds INTEGER,
            transcript TEXT,
            word_count INTEGER,
            provider TEXT,
            segments TEXT,
            frames_json TEXT,
            frame_count INTEGER DEFAULT 0,
            frame_interval_seconds INTEGER,
            frame_analysis TEXT,
            emotions TEXT,
            description TEXT,
            content_insights TEXT,
            original_transcribed_at TEXT,
            archived_at TEXT NOT NULL
        )
        """)

        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_script_video_id ON script_analysis(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_video_id ON affiliate_recommendations(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_video_id ON description_analysis(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_conversion_video_id ON conversion_analysis(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transcript_video_id ON video_transcripts(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transcript_history_video_id ON video_transcripts_history(video_id)")

        conn.commit()
        conn.close()

        # Run migrations for existing databases
        self._migrate_database()
        logger.info("Database tables initialized successfully")

    def _migrate_database(self):
        """Add new columns to existing database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check and add frame_analysis column
        try:
            cursor.execute("SELECT frame_analysis FROM video_transcripts LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding frame_analysis column to video_transcripts")
            cursor.execute("ALTER TABLE video_transcripts ADD COLUMN frame_analysis TEXT")

        # Check and add emotions column
        try:
            cursor.execute("SELECT emotions FROM video_transcripts LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding emotions column to video_transcripts")
            cursor.execute("ALTER TABLE video_transcripts ADD COLUMN emotions TEXT")

        # Check and add description column for YouTube video description
        try:
            cursor.execute("SELECT description FROM video_transcripts LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding description column to video_transcripts")
            cursor.execute("ALTER TABLE video_transcripts ADD COLUMN description TEXT")

        # Check and add content_insights column for multimodal AI analysis
        try:
            cursor.execute("SELECT content_insights FROM video_transcripts LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding content_insights column to video_transcripts")
            cursor.execute("ALTER TABLE video_transcripts ADD COLUMN content_insights TEXT")

        # Check and add YT Analytics columns to description_analysis
        yt_columns = [
            ('yt_total_views', 'INTEGER DEFAULT 0'),
            ('yt_total_impressions', 'INTEGER DEFAULT 0'),
            ('yt_overall_ctr', 'REAL DEFAULT 0.0'),
            ('yt_by_traffic_source', 'TEXT'),  # JSON array
            ('main_keyword', 'TEXT'),
            ('silo', 'TEXT')
        ]
        for col_name, col_type in yt_columns:
            try:
                cursor.execute(f"SELECT {col_name} FROM description_analysis LIMIT 1")
            except sqlite3.OperationalError:
                logger.info(f"Adding {col_name} column to description_analysis")
                cursor.execute(f"ALTER TABLE description_analysis ADD COLUMN {col_name} {col_type}")

        conn.commit()
        conn.close()

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
                optimization_suggestions, missing_elements, strengths,
                yt_total_views, yt_total_impressions, yt_overall_ctr,
                yt_by_traffic_source, main_keyword, silo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                # Handle new YT Analytics columns that may not exist in older records
                yt_total_views = 0
                yt_total_impressions = 0
                yt_overall_ctr = 0.0
                yt_by_traffic_source = []
                main_keyword = ""
                silo = ""

                try:
                    yt_total_views = row['yt_total_views'] or 0
                    yt_total_impressions = row['yt_total_impressions'] or 0
                    yt_overall_ctr = row['yt_overall_ctr'] or 0.0
                    yt_by_traffic_source = json.loads(row['yt_by_traffic_source']) if row['yt_by_traffic_source'] else []
                    main_keyword = row['main_keyword'] or ""
                    silo = row['silo'] or ""
                except (KeyError, IndexError):
                    pass

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

    # ==================== TRANSCRIPT METHODS ====================

    def store_transcript(self, video_id: str, title: str, channel: str,
                        duration_seconds: int, transcript: str, word_count: int,
                        provider: str, segments: list = None, frames: list = None,
                        frame_interval: int = None, frame_analysis: list = None,
                        emotions: dict = None, description: str = None,
                        content_insights: dict = None) -> bool:
        """Store video transcript, frame analysis, emotion data, description, and content insights.
        If existing data exists, it will be archived to history table first."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            now = datetime.now().isoformat()

            # Check if data already exists and archive it
            cursor.execute("SELECT * FROM video_transcripts WHERE video_id = ?", (video_id,))
            existing = cursor.fetchone()

            if existing:
                # Archive old data to history table
                logger.info(f"Archiving existing transcript data for video {video_id}")
                cursor.execute("""
                INSERT INTO video_transcripts_history (
                    video_id, title, channel, duration_seconds, transcript,
                    word_count, provider, segments, frames_json, frame_count,
                    frame_interval_seconds, frame_analysis, emotions, description,
                    content_insights, original_transcribed_at, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    existing['video_id'],
                    existing['title'],
                    existing['channel'],
                    existing['duration_seconds'],
                    existing['transcript'],
                    existing['word_count'],
                    existing['provider'],
                    existing['segments'],
                    existing['frames_json'],
                    existing['frame_count'],
                    existing['frame_interval_seconds'],
                    existing['frame_analysis'] if 'frame_analysis' in existing.keys() else None,
                    existing['emotions'] if 'emotions' in existing.keys() else None,
                    existing['description'] if 'description' in existing.keys() else None,
                    existing['content_insights'] if 'content_insights' in existing.keys() else None,
                    existing['transcribed_at'],
                    now
                ))
                logger.info(f"Archived transcript to history for video {video_id}")

            # Now insert/replace the new data
            cursor.execute("""
            INSERT OR REPLACE INTO video_transcripts (
                video_id, title, channel, duration_seconds, transcript,
                word_count, provider, segments, frames_json, frame_count,
                frame_interval_seconds, frame_analysis, emotions, description,
                content_insights, transcribed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM video_transcripts WHERE video_id = ?
            """, (video_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                result = {
                    'video_id': row['video_id'],
                    'title': row['title'],
                    'channel': row['channel'],
                    'duration_seconds': row['duration_seconds'],
                    'transcript': row['transcript'],
                    'word_count': row['word_count'],
                    'provider': row['provider'],
                    'segments': json.loads(row['segments']) if row['segments'] else None,
                    'frames': json.loads(row['frames_json']) if row['frames_json'] else None,
                    'frame_count': row['frame_count'],
                    'frame_interval_seconds': row['frame_interval_seconds'],
                    'transcribed_at': row['transcribed_at'],
                    'updated_at': row['updated_at']
                }
                # Add new fields if they exist in the database
                try:
                    result['frame_analysis'] = json.loads(row['frame_analysis']) if row['frame_analysis'] else None
                    result['emotions'] = json.loads(row['emotions']) if row['emotions'] else None
                except (KeyError, IndexError):
                    # Columns may not exist in older databases
                    result['frame_analysis'] = None
                    result['emotions'] = None

                # Add description field
                try:
                    result['description'] = row['description']
                except (KeyError, IndexError):
                    result['description'] = None

                # Add content_insights field (multimodal AI analysis)
                try:
                    result['content_insights'] = json.loads(row['content_insights']) if row['content_insights'] else None
                except (KeyError, IndexError):
                    result['content_insights'] = None

                return result
            return None

        except Exception as e:
            logger.error(f"Error fetching transcript: {str(e)}")
            return None

    def has_transcript(self, video_id: str) -> bool:
        """Check if a video has a stored transcript."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM video_transcripts WHERE video_id = ?", (video_id,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking transcript: {str(e)}")
            return False

    def delete_transcript(self, video_id: str) -> bool:
        """Delete transcript for a video (for re-transcription)."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM video_transcripts WHERE video_id = ?", (video_id,))
            conn.commit()
            conn.close()
            logger.info(f"Deleted transcript for video {video_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting transcript: {str(e)}")
            return False

    def get_all_transcripts(self, limit: int = 50) -> list:
        """Get all stored transcripts (for history view)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT video_id, title, channel, duration_seconds, word_count,
                   provider, frame_count, transcribed_at
            FROM video_transcripts
            ORDER BY transcribed_at DESC
            LIMIT ?
            """, (limit,))

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Error fetching transcripts: {str(e)}")
            return []

    def get_transcript_history(self, video_id: str, limit: int = 10) -> List[dict]:
        """Get historical transcript data for a video."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
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
            """, (video_id, limit))

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Error fetching transcript history: {str(e)}")
            return []

    def get_transcript_history_detail(self, history_id: int) -> Optional[dict]:
        """Get full details of a historical transcript entry."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT * FROM video_transcripts_history WHERE id = ?
            """, (history_id,))

            row = cursor.fetchone()
            conn.close()

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
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            now = datetime.now().isoformat()

            cursor.execute("""
            UPDATE video_transcripts
            SET content_insights = ?, updated_at = ?
            WHERE video_id = ?
            """, (
                json.dumps(content_insights) if content_insights else None,
                now,
                video_id
            ))

            if cursor.rowcount == 0:
                logger.warning(f"No transcript found to update for video {video_id}")
                conn.close()
                return False

            conn.commit()
            conn.close()
            logger.info(f"Updated content_insights for video {video_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating content_insights: {str(e)}")
            return False
