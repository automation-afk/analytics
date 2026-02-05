"""API blueprint - JSON endpoints for AJAX requests."""
from flask import Blueprint, jsonify, request, current_app
from app.extensions import cache
from app.blueprints.auth import login_required

bp = Blueprint('api', __name__)


@bp.route('/videos')
@login_required
@cache.cached(timeout=300, query_string=True)
def get_videos():
    """
    Get list of videos (JSON).

    Query parameters:
        - limit: Number of videos (default 25)
        - offset: Offset for pagination (default 0)
        - channel: Filter by channel code
        - has_analysis: Filter by analysis status (true/false)
    """
    limit = request.args.get('limit', 25, type=int)
    offset = request.args.get('offset', 0, type=int)
    channel_code = request.args.get('channel')
    has_analysis = request.args.get('has_analysis')

    # Convert has_analysis to boolean
    if has_analysis == 'true':
        has_analysis = True
    elif has_analysis == 'false':
        has_analysis = False
    else:
        has_analysis = None

    # Fetch videos
    videos = current_app.bigquery.get_videos(
        limit=limit,
        offset=offset,
        channel_code=channel_code,
        has_analysis=has_analysis
    )

    # Convert to dict
    videos_data = []
    for video in videos:
        videos_data.append({
            'video_id': video.video_id,
            'channel_code': video.channel_code,
            'title': video.title,
            'published_date': video.published_date.isoformat() if video.published_date else None,
            'video_url': video.video_url,
            'has_analysis': video.has_analysis,
            'latest_analysis_date': video.latest_analysis_date.isoformat() if video.latest_analysis_date else None
        })

    return jsonify({
        'videos': videos_data,
        'count': len(videos_data),
        'limit': limit,
        'offset': offset
    })


@bp.route('/videos/<video_id>')
@login_required
def get_video(video_id):
    """
    Get single video details (JSON).

    Args:
        video_id: YouTube video ID
    """
    video = current_app.bigquery.get_video_by_id(video_id)

    if not video:
        return jsonify({'error': 'Video not found'}), 404

    return jsonify({
        'video_id': video.video_id,
        'channel_code': video.channel_code,
        'title': video.title,
        'published_date': video.published_date.isoformat() if video.published_date else None,
        'video_url': video.video_url,
        'description': video.description,
        'has_analysis': video.has_analysis,
        'latest_analysis_date': video.latest_analysis_date.isoformat() if video.latest_analysis_date else None
    })


@bp.route('/analysis/<video_id>')
@login_required
def get_analysis(video_id):
    """
    Get latest analysis results for a video (JSON).

    Args:
        video_id: YouTube video ID
    """
    analysis = current_app.bigquery.get_latest_analysis(video_id)

    if not analysis or not analysis.video:
        return jsonify({'error': 'Analysis not found'}), 404

    result = {
        'video_id': video_id,
        'video': {
            'title': analysis.video.title,
            'channel_code': analysis.video.channel_code
        }
    }

    # Add script analysis if available
    if analysis.script_analysis:
        result['script_analysis'] = {
            'script_quality_score': analysis.script_analysis.script_quality_score,
            'hook_effectiveness_score': analysis.script_analysis.hook_effectiveness_score,
            'call_to_action_score': analysis.script_analysis.call_to_action_score,
            'persuasion_effectiveness_score': analysis.script_analysis.persuasion_effectiveness_score,
            'user_intent_match_score': analysis.script_analysis.user_intent_match_score,
            'key_strengths': analysis.script_analysis.key_strengths,
            'improvement_areas': analysis.script_analysis.improvement_areas
        }

    # Add revenue metrics if available
    if analysis.revenue_metrics:
        result['revenue_metrics'] = {
            'revenue': analysis.revenue_metrics.revenue,
            'clicks': analysis.revenue_metrics.clicks,
            'sales': analysis.revenue_metrics.sales,
            'views': analysis.revenue_metrics.organic_views,
            'conversion_rate': analysis.revenue_metrics.conversion_rate,
            'revenue_per_click': analysis.revenue_metrics.revenue_per_click
        }

    # Add affiliate recommendations if available
    if analysis.affiliate_recommendations:
        result['affiliate_recommendations'] = [
            {
                'product_rank': rec.product_rank,
                'product_name': rec.product_name,
                'relevance_score': rec.relevance_score,
                'conversion_probability': rec.conversion_probability,
                'recommendation_reasoning': rec.recommendation_reasoning
            }
            for rec in analysis.affiliate_recommendations[:5]
        ]

    return jsonify(result)


@bp.route('/dashboard/stats')
@login_required
@cache.cached(timeout=300)
def get_dashboard_stats():
    """
    Get dashboard statistics (JSON).
    """
    stats = current_app.bigquery.get_dashboard_stats()

    return jsonify({
        'total_videos': stats.total_videos,
        'analyzed_videos': stats.analyzed_videos,
        'avg_script_quality': stats.avg_script_quality,
        'avg_hook_score': stats.avg_hook_score,
        'avg_cta_score': stats.avg_cta_score,
        'avg_conversion_rate': stats.avg_conversion_rate,
        'total_revenue': stats.total_revenue,
        'total_views': stats.total_videos,
        'avg_revenue_per_video': stats.avg_revenue_per_video
    })


@bp.route('/analysis/status')
@login_required
def get_analysis_status():
    """
    Check if any analyses are currently running.

    Returns:
        {
            "is_analyzing": true/false,
            "videos_analyzing": [
                {"video_id": "...", "title": "..."},
                ...
            ]
        }
    """
    # Check all videos in cache for analyzing flag
    videos_analyzing = []

    # Get more videos to increase chance of finding analyzing ones
    recent_videos = current_app.bigquery.get_videos(limit=200)

    for video in recent_videos:
        is_analyzing = cache.get(f'analyzing_{video.video_id}')
        if is_analyzing:
            videos_analyzing.append({
                'video_id': video.video_id,
                'title': video.title,
                'channel_code': video.channel_code
            })

    return jsonify({
        'is_analyzing': len(videos_analyzing) > 0,
        'videos_analyzing': videos_analyzing,
        'count': len(videos_analyzing)
    })


@bp.route('/analysis/trigger', methods=['POST'])
@login_required
def trigger_analysis():
    """
    Trigger analysis (JSON endpoint).

    Request body:
        {
            "video_id": "video_id",
            "analysis_types": ["script", "description", "affiliate", "conversion"]
        }

    Returns:
        {"job_id": "uuid", "status": "pending"}
    """
    data = request.get_json()
    video_id = data.get('video_id')
    analysis_types = data.get('analysis_types', ['script', 'description', 'affiliate', 'conversion'])

    if not video_id:
        return jsonify({'error': 'video_id is required'}), 400

    # Import here to avoid circular dependency
    from app.services.analysis_service import AnalysisService
    import uuid

    analysis_service = AnalysisService(
        bigquery_service=current_app.bigquery,
        anthropic_api_key=current_app.config['ANTHROPIC_API_KEY']
    )

    job_id = str(uuid.uuid4())

    try:
        # Run analysis
        result = analysis_service.analyze_video(video_id, analysis_types)

        return jsonify({
            'job_id': job_id,
            'status': 'completed',
            'video_id': video_id,
            'message': 'Analysis completed successfully'
        })

    except Exception as e:
        return jsonify({
            'job_id': job_id,
            'status': 'failed',
            'error': str(e)
        }), 500
