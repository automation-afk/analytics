"""Analysis blueprint - trigger and monitor analysis jobs."""
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session
import uuid
from datetime import datetime
from app.blueprints.auth import login_required

bp = Blueprint('analysis', __name__, url_prefix='/analysis')

# In-memory job storage (use Redis or database in production)
jobs = {}


@bp.route('/trigger', methods=['GET'])
@login_required
def trigger_form():
    """Show analysis trigger form."""
    # Log page view
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_view_analysis_page(email)

    return render_template('analysis/trigger.html')


@bp.route('/trigger', methods=['POST'])
@login_required
def trigger_analysis():
    """
    Execute batch analysis based on form input.
    """
    from app.services.analysis_service import AnalysisService

    # Get form data
    video_id = request.form.get('video_id')
    channel_code = request.form.get('channel_code')
    unanalyzed_only = request.form.get('unanalyzed_only') == 'on'
    analysis_types = request.form.getlist('analysis_types')

    if not analysis_types:
        analysis_types = ['script', 'description', 'affiliate', 'conversion']

    # Get videos to analyze
    video_ids = []

    if video_id:
        # Single video
        video_ids = [video_id.strip()]
    else:
        # Batch by filter
        has_analysis = False if unanalyzed_only else None
        videos = current_app.bigquery.get_videos(
            limit=100,  # Max 100 videos per batch
            channel_code=channel_code,
            has_analysis=has_analysis
        )
        video_ids = [v.video_id for v in videos]

    if not video_ids:
        flash('No videos found matching criteria', 'warning')
        return redirect(url_for('analysis.trigger_form'))

    # Log analysis trigger
    email = session.get('user_email')
    if email and current_app.activity_logger:
        if video_id:
            current_app.activity_logger.log_start_analysis(email, video_id, analysis_types)
        else:
            current_app.activity_logger.log_batch_analysis(email, len(video_ids), channel_code)

    # Create analysis service
    analysis_service = AnalysisService(
        bigquery_service=current_app.bigquery,
        anthropic_api_key=current_app.config['ANTHROPIC_API_KEY']
    )

    # Create job
    job_id = str(uuid.uuid4())
    job = {
        'job_id': job_id,
        'status': 'running',
        'video_ids': video_ids,
        'analysis_types': analysis_types,
        'progress': 0,
        'total_videos': len(video_ids),
        'processed_videos': 0,
        'started_at': datetime.now(),
        'current_video': None,
        'error_message': None
    }
    jobs[job_id] = job

    # Run analysis in background (simplified - should use Celery/APScheduler)
    # For now, we'll run synchronously with progress updates
    try:
        for idx, vid in enumerate(video_ids):
            job['current_video'] = vid
            job['processed_videos'] = idx
            job['progress'] = int((idx / len(video_ids)) * 100)

            # Run analysis
            analysis_service.analyze_video(vid, analysis_types)

        # Mark as completed
        job['status'] = 'completed'
        job['progress'] = 100
        job['processed_videos'] = len(video_ids)
        job['completed_at'] = datetime.now()

        flash(f'Analysis completed for {len(video_ids)} videos', 'success')

    except Exception as e:
        job['status'] = 'failed'
        job['error_message'] = str(e)
        flash(f'Analysis failed: {str(e)}', 'error')

    return redirect(url_for('analysis.status', job_id=job_id))


@bp.route('/status/<job_id>')
@login_required
def status(job_id):
    """
    Show analysis job status.

    Args:
        job_id: Job ID
    """
    job = jobs.get(job_id)

    if not job:
        flash('Job not found', 'error')
        return redirect(url_for('dashboard.overview'))

    return render_template('analysis/results.html', job=job)


@bp.route('/history')
@login_required
def history():
    """Show analysis history from database."""
    # Log page view
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_view_history(email)

    import sqlite3

    # Get analysis history from local database
    db_path = current_app.local_db.db_path
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get recent analyses
    cursor.execute("""
    SELECT
        video_id,
        analysis_timestamp,
        script_quality_score,
        hook_effectiveness_score,
        call_to_action_score
    FROM script_analysis
    ORDER BY analysis_timestamp DESC
    LIMIT 50
    """)

    analyses = []
    for row in cursor.fetchall():
        analyses.append({
            'video_id': row['video_id'],
            'timestamp': row['analysis_timestamp'],
            'script_score': row['script_quality_score'],
            'hook_score': row['hook_effectiveness_score'],
            'cta_score': row['call_to_action_score']
        })

    conn.close()

    return render_template('analysis/history.html', analyses=analyses, jobs=[])
