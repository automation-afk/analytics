#!/usr/bin/env python3
"""Direct migration script with embedded DATABASE_URL."""
import os
import sys
import sqlite3

# Set DATABASE_URL directly
os.environ['DATABASE_URL'] = 'postgresql://postgres:fNqgPnDjwyEPXneRIBmWBxneKSMRdaOI@yamabiko.proxy.rlwy.net:32154/railway'

def get_sqlite_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_postgres_connection(database_url):
    import psycopg2
    return psycopg2.connect(database_url)

def migrate_table(sqlite_conn, pg_conn, table_name, columns):
    sqlite_cursor = sqlite_conn.cursor()
    pg_cursor = pg_conn.cursor()

    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cursor.fetchall()

    if not rows:
        print(f"  No data in {table_name}")
        return 0

    col_names = [description[0] for description in sqlite_cursor.description]
    valid_cols = [c for c in col_names if c != 'id' and c in columns]

    if not valid_cols:
        print(f"  No matching columns for {table_name}")
        return 0

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
            continue

    pg_conn.commit()
    return migrated

def main():
    database_url = os.getenv('DATABASE_URL')
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sqlite_path = os.path.join(base_dir, 'data', 'analysis.db')

    print(f"Source: {sqlite_path}")
    print(f"Target: PostgreSQL (Railway)")
    print()

    try:
        sqlite_conn = get_sqlite_connection(sqlite_path)
        print("Connected to SQLite")
    except Exception as e:
        print(f"ERROR: SQLite: {e}")
        sys.exit(1)

    try:
        pg_conn = get_postgres_connection(database_url)
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"ERROR: PostgreSQL: {e}")
        sys.exit(1)

    print()
    print("Starting migration...")
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

    total_migrated = 0
    for table_name, columns in tables.items():
        print(f"Migrating {table_name}...")
        try:
            count = migrate_table(sqlite_conn, pg_conn, table_name, columns)
            print(f"  Migrated {count} rows")
            total_migrated += count
        except Exception as e:
            print(f"  ERROR: {e}")

    print()
    print(f"Migration complete! Total rows: {total_migrated}")

    sqlite_conn.close()
    pg_conn.close()

if __name__ == '__main__':
    main()
