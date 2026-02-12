#!/usr/bin/env python3
"""Initialize PostgreSQL tables and migrate data from SQLite."""
import os
import sys
import sqlite3

# Set DATABASE_URL
os.environ['DATABASE_URL'] = 'postgresql://postgres:fNqgPnDjwyEPXneRIBmWBxneKSMRdaOI@yamabiko.proxy.rlwy.net:32154/railway'

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_tables(pg_conn):
    """Create all tables in PostgreSQL."""
    cursor = pg_conn.cursor()

    print("Creating tables...")

    # Script Analysis
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS script_analysis (
        id SERIAL PRIMARY KEY,
        video_id TEXT NOT NULL,
        channel_code TEXT,
        analysis_timestamp TEXT NOT NULL,
        script_quality_score REAL,
        hook_effectiveness_score REAL,
        call_to_action_score REAL,
        persuasion_effectiveness_score REAL,
        user_intent_match_score REAL,
        persuasion_techniques TEXT,
        key_strengths TEXT,
        improvement_areas TEXT,
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
    print("  Created script_analysis")

    # Affiliate Recommendations
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS affiliate_recommendations (
        id SERIAL PRIMARY KEY,
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
    print("  Created affiliate_recommendations")

    # Description Analysis
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS description_analysis (
        id SERIAL PRIMARY KEY,
        video_id TEXT NOT NULL,
        analysis_timestamp TEXT NOT NULL,
        cta_effectiveness_score REAL,
        description_quality_score REAL,
        seo_score REAL,
        total_links INTEGER,
        affiliate_links INTEGER,
        link_positioning_score REAL,
        has_clear_cta INTEGER,
        optimization_suggestions TEXT,
        missing_elements TEXT,
        strengths TEXT,
        yt_total_views INTEGER DEFAULT 0,
        yt_total_impressions INTEGER DEFAULT 0,
        yt_overall_ctr REAL DEFAULT 0.0,
        yt_by_traffic_source TEXT,
        main_keyword TEXT,
        silo TEXT,
        UNIQUE(video_id, analysis_timestamp)
    )
    """)
    print("  Created description_analysis")

    # Conversion Analysis
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS conversion_analysis (
        id SERIAL PRIMARY KEY,
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
        conversion_drivers TEXT,
        underperformance_reasons TEXT,
        recommendations TEXT,
        UNIQUE(video_id, analysis_timestamp)
    )
    """)
    print("  Created conversion_analysis")

    # Video Transcripts
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS video_transcripts (
        id SERIAL PRIMARY KEY,
        video_id TEXT NOT NULL UNIQUE,
        title TEXT,
        channel TEXT,
        duration_seconds INTEGER,
        transcript TEXT NOT NULL,
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
        transcribed_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)
    print("  Created video_transcripts")

    # Transcript History
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS video_transcripts_history (
        id SERIAL PRIMARY KEY,
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
    print("  Created video_transcripts_history")

    # Create indexes
    indexes = [
        ("idx_script_video_id", "script_analysis", "video_id"),
        ("idx_affiliate_video_id", "affiliate_recommendations", "video_id"),
        ("idx_description_video_id", "description_analysis", "video_id"),
        ("idx_conversion_video_id", "conversion_analysis", "video_id"),
        ("idx_transcript_video_id", "video_transcripts", "video_id"),
        ("idx_transcript_history_video_id", "video_transcripts_history", "video_id"),
    ]
    for idx_name, table, column in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
        except:
            pass
    print("  Created indexes")

    pg_conn.commit()
    print("Tables created successfully!")
    print()

def migrate_table(sqlite_conn, pg_conn, table_name, columns):
    """Migrate data from SQLite to PostgreSQL."""
    sqlite_cursor = sqlite_conn.cursor()
    pg_cursor = pg_conn.cursor()

    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cursor.fetchall()

    if not rows:
        print(f"  No data in {table_name}")
        return 0

    col_names = [description[0] for description in sqlite_cursor.description]
    valid_cols = [c for c in col_names if c != 'id' and c in columns]

    placeholders = ', '.join(['%s'] * len(valid_cols))
    col_list = ', '.join(valid_cols)
    insert_query = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    migrated = 0
    for row in rows:
        row_dict = dict(row)
        values = [row_dict.get(col) for col in valid_cols]
        try:
            pg_cursor.execute(insert_query, values)
            migrated += 1
        except Exception as e:
            print(f"  Warning: {e}")
            pg_conn.rollback()

    pg_conn.commit()
    return migrated

def main():
    import psycopg2

    database_url = os.getenv('DATABASE_URL')
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sqlite_path = os.path.join(base_dir, 'data', 'analysis.db')

    print(f"Source: {sqlite_path}")
    print(f"Target: PostgreSQL (Railway)")
    print()

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    print("Connected to SQLite")

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(database_url)
    print("Connected to PostgreSQL")
    print()

    # Create tables first
    create_tables(pg_conn)

    # Migrate data
    print("Migrating data...")
    print()

    tables = {
        'script_analysis': [
            'video_id', 'channel_code', 'analysis_timestamp',
            'script_quality_score', 'hook_effectiveness_score', 'call_to_action_score',
            'persuasion_effectiveness_score', 'user_intent_match_score',
            'persuasion_techniques', 'key_strengths', 'improvement_areas',
            'target_audience', 'content_value_score', 'identified_intent',
            'has_clear_intro', 'has_clear_cta', 'problem_solution_structure',
            'readability_score'
        ],
        'affiliate_recommendations': [
            'video_id', 'recommendation_timestamp', 'product_rank',
            'product_name', 'product_category', 'relevance_score',
            'conversion_probability', 'recommendation_reasoning',
            'where_to_mention', 'mentioned_in_video', 'amazon_asin', 'price_range'
        ],
        'description_analysis': [
            'video_id', 'analysis_timestamp', 'cta_effectiveness_score',
            'description_quality_score', 'seo_score', 'total_links',
            'affiliate_links', 'link_positioning_score', 'has_clear_cta',
            'optimization_suggestions', 'missing_elements', 'strengths',
            'yt_total_views', 'yt_total_impressions', 'yt_overall_ctr',
            'yt_by_traffic_source', 'main_keyword', 'silo'
        ],
        'conversion_analysis': [
            'video_id', 'analysis_timestamp', 'metrics_date',
            'revenue', 'clicks', 'sales', 'views',
            'conversion_rate', 'revenue_per_click', 'revenue_per_1k_views',
            'conversion_drivers', 'underperformance_reasons', 'recommendations'
        ],
        'video_transcripts': [
            'video_id', 'title', 'channel', 'duration_seconds', 'transcript',
            'word_count', 'provider', 'segments', 'frames_json', 'frame_count',
            'frame_interval_seconds', 'frame_analysis', 'emotions', 'description',
            'content_insights', 'transcribed_at', 'updated_at'
        ],
        'video_transcripts_history': [
            'video_id', 'title', 'channel', 'duration_seconds', 'transcript',
            'word_count', 'provider', 'segments', 'frames_json', 'frame_count',
            'frame_interval_seconds', 'frame_analysis', 'emotions', 'description',
            'content_insights', 'original_transcribed_at', 'archived_at'
        ]
    }

    total = 0
    for table_name, columns in tables.items():
        print(f"Migrating {table_name}...")
        count = migrate_table(sqlite_conn, pg_conn, table_name, columns)
        print(f"  Migrated {count} rows")
        total += count

    print()
    print(f"=== Migration complete! Total rows: {total} ===")
    print()
    print("Next: Add DATABASE_URL to your web app on Railway!")

    sqlite_conn.close()
    pg_conn.close()

if __name__ == '__main__':
    main()
