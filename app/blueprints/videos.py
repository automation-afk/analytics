"""Videos blueprint - individual video detail pages."""
from flask import Blueprint, render_template, redirect, url_for, flash, current_app, request, session
from app.extensions import cache
from app.blueprints.auth import login_required

bp = Blueprint('videos', __name__, url_prefix='/videos')


def compute_optimization_opportunity(revenue_potential, current_revenue, quality_score):
    """Compute optimization opportunity: (Revenue Potential - Current Revenue) x (100 - Quality Score) / 100"""
    gap = max((revenue_potential or 0) - (current_revenue or 0), 0)
    return round(gap * (100 - (quality_score or 0)) / 100, 2)


@bp.route('/<video_id>')
@login_required
def detail(video_id):
    """
    Video detail page showing all analysis results.

    Args:
        video_id: YouTube video ID
    """
    # Check if analysis is in progress
    is_analyzing = cache.get(f'analyzing_{video_id}') or False
    is_transcribing = cache.get(f'transcribing_{video_id}') or False

    # Cache key for this video's analysis data
    cache_key = f'video_detail_{video_id}'

    # Allow force refresh with ?refresh=1
    force_refresh = request.args.get('refresh') == '1'
    if force_refresh:
        cache.delete(cache_key)

    # Only use cache if no operation is in progress and not forcing refresh
    if not is_analyzing and not is_transcribing and not force_refresh:
        cached_analysis = cache.get(cache_key)
        if cached_analysis:
            analysis = cached_analysis
        else:
            analysis = current_app.bigquery.get_latest_analysis(video_id)
            if analysis and analysis.video:
                # Cache for 2 minutes (reduced from 5)
                cache.set(cache_key, analysis, timeout=120)
    else:
        # Operation in progress or force refresh - always fetch fresh data
        analysis = current_app.bigquery.get_latest_analysis(video_id)
        # Update cache with fresh data
        if analysis and analysis.video and not is_analyzing and not is_transcribing:
            cache.set(cache_key, analysis, timeout=120)

    if not analysis or not analysis.video:
        flash(f'Video not found: {video_id}', 'error')
        return redirect(url_for('dashboard.videos_list'))

    # Log video detail view
    email = session.get('user_email')
    if email and current_app.activity_logger:
        video_title = analysis.video.title if analysis.video else None
        current_app.activity_logger.log_view_video_detail(email, video_id, video_title)

    # Get transcript data from local database (includes emotions, frame analysis)
    transcript_data = None
    if current_app.local_db:
        transcript_data = current_app.local_db.get_transcript(video_id)

    # Get script score from local database
    script_score = None
    if current_app.local_db:
        script_score = current_app.local_db.get_script_score(video_id)

    # Compute optimization opportunity if script score exists
    optimization_opportunity = None
    opt_opp_details = None
    if script_score and script_score.get('quality_score_total') is not None:
        try:
            meta = current_app.bigquery.get_video_metadata_batch([video_id])
            if video_id in meta:
                m = meta[video_id]
                rev_potential = m['revenue_potential']
                current_rev = m['avg_monthly_revenue']
                quality = script_score['quality_score_total']
                optimization_opportunity = compute_optimization_opportunity(
                    rev_potential, current_rev, quality
                )
                opt_opp_details = {
                    'revenue_potential': rev_potential,
                    'current_revenue': current_rev,
                    'quality_score': quality,
                    'best_video_id': m.get('best_video_id', ''),
                    'keyword': m.get('main_keyword', ''),
                }
        except Exception:
            pass

    return render_template(
        'videos/detail.html',
        analysis=analysis,
        video=analysis.video,
        revenue_metrics=analysis.revenue_metrics,
        script_analysis=analysis.script_analysis,
        affiliate_recs=analysis.affiliate_recommendations,
        description_analysis=analysis.description_analysis,
        conversion_analysis=analysis.conversion_analysis,
        affiliate_performance=analysis.affiliate_performance,
        existing_links=analysis.existing_links_analysis,
        transcript_data=transcript_data,
        script_score=script_score,
        optimization_opportunity=optimization_opportunity,
        opt_opp_details=opt_opp_details,
        is_analyzing=is_analyzing,
        is_transcribing=is_transcribing
    )


@bp.route('/<video_id>/analyze', methods=['POST'])
@login_required
def analyze_single(video_id):
    """
    Trigger analysis for a single video (runs in background).

    Args:
        video_id: YouTube video ID
    """
    # Import here to avoid circular dependency
    from app.services.analysis_service import AnalysisService
    import threading

    # CRITICAL: Get app reference BEFORE defining thread function
    # Must be done while still in request context
    app = current_app._get_current_object()

    def run_analysis():
        """Run analysis in background thread."""
        import logging
        logger = logging.getLogger(__name__)

        try:
            with app.app_context():
                # Create analysis service INSIDE the app context
                analysis_service = AnalysisService(
                    bigquery_service=app.bigquery,
                    anthropic_api_key=app.config['ANTHROPIC_API_KEY']
                )

                # Run analysis
                result = analysis_service.analyze_video(
                    video_id=video_id,
                    analysis_types=['script', 'description', 'affiliate', 'conversion']
                )
                # Invalidate cache for this video
                cache.delete(f'video_detail_{video_id}')
                # Clear analyzing flag
                cache.delete(f'analyzing_{video_id}')
                logger.info(f'Background analysis completed for {video_id}')
        except Exception as e:
            logger.error(f'Background analysis failed for {video_id}: {str(e)}')
            # Clear analyzing flag even on error
            try:
                with app.app_context():
                    cache.delete(f'analyzing_{video_id}')
            except:
                # If clearing flag fails, it will expire after 10 minutes anyway
                pass

    # Log analysis start
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_start_analysis(
            email, video_id, ['script', 'description', 'affiliate', 'conversion']
        )

    # Set analyzing flag in cache (expires in 10 minutes)
    cache.set(f'analyzing_{video_id}', True, timeout=600)

    # Start analysis in background thread
    thread = threading.Thread(target=run_analysis)
    thread.daemon = True
    thread.start()

    # Redirect immediately with message
    flash(f'Analysis started! This takes 2-3 minutes. Refresh this page to see results.', 'info')
    return redirect(url_for('videos.detail', video_id=video_id))
