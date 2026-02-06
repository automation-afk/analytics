"""Dashboard blueprint - main overview and video list pages."""
from flask import Blueprint, render_template, request, current_app, redirect, url_for, session
from app.extensions import cache
from app.blueprints.auth import login_required

bp = Blueprint('dashboard', __name__)


@bp.route('/')
def index():
    """Landing page - redirect to login or dashboard."""
    from flask import session
    if 'user_email' in session:
        # User is authenticated, go to dashboard
        return redirect(url_for('dashboard.overview'))
    else:
        # User not authenticated, go to login
        return redirect(url_for('auth.login'))


@bp.route('/dashboard')
@login_required
@cache.cached(timeout=300)  # Cache for 5 minutes
def overview():
    """
    Main dashboard overview page.
    Shows KPI cards, charts, and recent analyses.
    """
    # Log dashboard view
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_view_dashboard(email)

    # Get dashboard statistics from BigQuery
    stats = current_app.bigquery.get_dashboard_stats()

    # Get recent videos (top 10)
    recent_videos = current_app.bigquery.get_videos(limit=10, has_analysis=True)

    return render_template(
        'dashboard/overview.html',
        stats=stats,
        recent_videos=recent_videos
    )


@bp.route('/dashboard/videos')
@login_required
def videos_list():
    """
    Video list page with filtering and pagination.
    """
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = current_app.config['VIDEOS_PER_PAGE']
    channel_code = request.args.get('channel')
    video_id = request.args.get('video_id')
    has_analysis = request.args.get('has_analysis')

    # Log videos list view
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_view_videos_list(email, {
            'channel': channel_code,
            'video_id': video_id,
            'has_analysis': has_analysis,
            'page': page
        })

    # Convert has_analysis to boolean
    if has_analysis == 'true':
        has_analysis = True
    elif has_analysis == 'false':
        has_analysis = False
    else:
        has_analysis = None

    # Calculate offset
    offset = (page - 1) * per_page

    # Create cache key for this specific query
    cache_key = f'videos_list_{page}_{channel_code}_{video_id}_{has_analysis}'
    cached_videos = cache.get(cache_key)

    if cached_videos is not None:
        videos = cached_videos
    else:
        # Fetch videos from BigQuery
        videos = current_app.bigquery.get_videos(
            limit=per_page,
            offset=offset,
            channel_code=channel_code,
            video_id=video_id,
            has_analysis=has_analysis
        )
        # Cache for 2 minutes
        cache.set(cache_key, videos, timeout=120)

    # Check which videos are currently being analyzed
    analyzing_videos = {}
    for video in videos:
        analyzing_videos[video.video_id] = cache.get(f'analyzing_{video.video_id}') or False

    # Get all available channels for dropdown (cached for 10 minutes)
    all_channels = cache.get('all_channels')
    if all_channels is None:
        all_channels = current_app.bigquery.get_all_channels()
        cache.set('all_channels', all_channels, timeout=600)

    # Get total count for pagination (simplified)
    has_more = len(videos) == per_page

    return render_template(
        'dashboard/videos.html',
        videos=videos,
        page=page,
        has_more=has_more,
        channel_code=channel_code,
        video_id_filter=video_id,
        has_analysis_filter=has_analysis,
        analyzing_videos=analyzing_videos,
        all_channels=all_channels
    )
