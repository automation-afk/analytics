"""API blueprint - JSON endpoints for AJAX requests."""
from flask import Blueprint, jsonify, request, current_app, session
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
    Uses a separate cache key to track analyzing videos (no BigQuery calls).

    Returns:
        {
            "is_analyzing": true/false,
            "videos_analyzing": [
                {"video_id": "...", "title": "...", "progress": ..., "message": ...},
                ...
            ]
        }
    """
    # Get list of analyzing videos from cache (no BigQuery!)
    analyzing_list = cache.get('analyzing_videos_list') or []

    # Filter to only those still marked as analyzing
    videos_analyzing = []
    for video_info in analyzing_list:
        video_id = video_info.get('video_id')
        if cache.get(f'analyzing_{video_id}'):
            # Add progress info if available
            progress_data = cache.get(f'analysis_progress_{video_id}') or {}
            videos_analyzing.append({
                'video_id': video_id,
                'title': video_info.get('title', ''),
                'channel_code': video_info.get('channel_code', ''),
                'progress': progress_data.get('progress', 0),
                'message': progress_data.get('message', 'Analyzing...'),
                'step': progress_data.get('step', '')
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
    Trigger analysis (JSON endpoint) - runs in background.

    Request body:
        {
            "video_id": "video_id",
            "analysis_types": ["script", "description", "affiliate", "conversion"]
        }

    Returns:
        {"status": "started", "video_id": "..."}
    """
    data = request.get_json()
    video_id = data.get('video_id')
    analysis_types = data.get('analysis_types', ['script', 'description', 'affiliate', 'conversion'])

    if not video_id:
        return jsonify({'error': 'video_id is required'}), 400

    # Log analysis start
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_start_analysis(email, video_id, analysis_types)

    # Check if already analyzing
    if cache.get(f'analyzing_{video_id}'):
        return jsonify({
            'status': 'already_running',
            'message': 'Analysis is already in progress for this video'
        })

    import threading

    cache.set(f'analyzing_{video_id}', True, timeout=600)  # 10 min timeout
    cache.set(f'analysis_progress_{video_id}', {
        'step': 'starting',
        'progress': 0,
        'message': 'Starting analysis...'
    }, timeout=600)

    # Get video info and add to analyzing list (for status polling without BigQuery)
    video_info = {'video_id': video_id, 'title': '', 'channel_code': ''}
    try:
        analysis = current_app.bigquery.get_latest_analysis(video_id)
        if analysis and analysis.video:
            video_info['title'] = analysis.video.title
            video_info['channel_code'] = analysis.video.channel_code
    except:
        pass

    analyzing_list = cache.get('analyzing_videos_list') or []
    # Remove if already in list, then add fresh
    analyzing_list = [v for v in analyzing_list if v.get('video_id') != video_id]
    analyzing_list.append(video_info)
    cache.set('analyzing_videos_list', analyzing_list, timeout=700)  # Slightly longer than analysis timeout

    app = current_app._get_current_object()

    def run_analysis():
        def progress_callback(step, progress, message):
            """Update progress in cache."""
            with app.app_context():
                cache.set(f'analysis_progress_{video_id}', {
                    'step': step,
                    'progress': progress,
                    'message': message
                }, timeout=600)

        try:
            with app.app_context():
                from app.services.analysis_service import AnalysisService

                analysis_service = AnalysisService(
                    bigquery_service=app.bigquery,
                    anthropic_api_key=app.config['ANTHROPIC_API_KEY']
                )

                # Calculate progress increments based on selected types
                total_steps = len(analysis_types)
                step_progress = 80 // max(total_steps, 1)  # Reserve 20% for start/end

                progress_callback('starting', 5, 'Fetching video data...')

                # Run analysis with progress updates
                result = analysis_service.analyze_video(
                    video_id,
                    analysis_types,
                    progress_callback=progress_callback
                )

                if result:
                    progress_callback('completed', 100, 'Analysis complete!')
                else:
                    cache.set(f'analysis_error_{video_id}', 'Analysis failed. Check server logs.', timeout=300)

        except Exception as e:
            import traceback
            import logging
            logger = logging.getLogger(__name__)
            error_msg = str(e)
            logger.error(f"Analysis error for {video_id}: {error_msg}")
            logger.error(traceback.format_exc())
            with app.app_context():
                cache.set(f'analysis_error_{video_id}', error_msg, timeout=300)
        finally:
            with app.app_context():
                cache.delete(f'analyzing_{video_id}')
                cache.delete(f'analysis_progress_{video_id}')
                cache.delete(f'video_detail_{video_id}')  # Invalidate video cache

    thread = threading.Thread(target=run_analysis)
    thread.daemon = True
    thread.start()

    return jsonify({
        'status': 'started',
        'video_id': video_id,
        'message': 'Analysis started in background'
    })


@bp.route('/analysis/status/<video_id>')
@login_required
def get_analysis_status_by_video(video_id):
    """
    Get the current status and progress of an analysis job for a specific video.

    Args:
        video_id: YouTube video ID

    Returns:
        {
            "status": "processing|completed|error|not_found",
            "step": "starting|script|description|affiliate|conversion|completed",
            "progress": 0-100,
            "message": "...",
            "error": "error message if status is error"
        }
    """
    # Check for errors first
    error_msg = cache.get(f'analysis_error_{video_id}')
    if error_msg:
        cache.delete(f'analysis_error_{video_id}')
        return jsonify({
            'status': 'error',
            'step': None,
            'progress': 0,
            'error': error_msg
        })

    # Check if analyzing
    is_analyzing = cache.get(f'analyzing_{video_id}')

    if not is_analyzing:
        return jsonify({
            'status': 'not_running',
            'step': None,
            'progress': 0
        })

    # Get progress details from cache
    progress_data = cache.get(f'analysis_progress_{video_id}') or {}

    return jsonify({
        'status': 'processing',
        'step': progress_data.get('step', 'starting'),
        'progress': progress_data.get('progress', 5),
        'message': progress_data.get('message', 'Processing...')
    })


# ==================== TRANSCRIPTION ENDPOINTS ====================

@bp.route('/transcribe', methods=['POST'])
@login_required
def transcribe_video():
    """
    Transcribe a YouTube video with optional AI analysis.

    Request body:
        {
            "video_id": "video_id",
            "generate_transcript": true,  // Generate new transcript (downloads video)
            "analyze_emotions": true,     // Generate new emotion analysis
            "analyze_frames": true,       // Generate new frame analysis
            "generate_insights": true,    // Generate AI content insights (uses new + existing data)
            "frame_interval": 10,         // Seconds between frames (if analyze_frames=true)
            "store_segments": false       // Include timestamp segments
        }

    Returns:
        {"status": "started", "video_id": "..."}
    """
    data = request.get_json()
    video_id = data.get('video_id')
    generate_transcript = data.get('generate_transcript', True)
    analyze_emotions = data.get('analyze_emotions', False)
    analyze_frames = data.get('analyze_frames', False)
    generate_insights = data.get('generate_insights', True)
    frame_interval = data.get('frame_interval')
    store_segments = data.get('store_segments', False)

    if not video_id:
        return jsonify({'error': 'video_id is required'}), 400

    # Validate at least one option is selected
    if not generate_transcript and not analyze_emotions and not analyze_frames and not generate_insights:
        return jsonify({'error': 'At least one option must be selected'}), 400

    import os
    import logging
    logger = logging.getLogger(__name__)

    # Get existing data
    existing = current_app.local_db.get_transcript(video_id)

    # Check if we need to download video (transcript, emotions, or frames require it)
    needs_download = generate_transcript or analyze_emotions or analyze_frames

    # If only generating insights and no existing transcript data, error
    if not needs_download and generate_insights and not existing:
        return jsonify({'error': 'No existing data found. Enable transcript, emotions, or frames to download video first.'}), 400

    # If only generating insights from existing data (no download needed)
    if not needs_download and generate_insights and existing:
        # Just regenerate insights from existing data
        try:
            from app.services.multimodal_analyzer import MultimodalAnalyzer
            anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')

            if not anthropic_api_key:
                return jsonify({'error': 'ANTHROPIC_API_KEY not configured'}), 500

            logger.info(f"Generating insights for {video_id} from existing data")
            logger.info(f"  - Has transcript: {bool(existing.get('transcript'))}")
            logger.info(f"  - Has emotions: {bool(existing.get('emotions'))}")
            logger.info(f"  - Has frames: {bool(existing.get('frame_analysis'))}")

            analyzer = MultimodalAnalyzer(anthropic_api_key)
            insights = analyzer.analyze_content(
                transcript=existing.get('transcript', ''),
                emotions=existing.get('emotions'),
                frame_analysis=existing.get('frame_analysis'),
                title=existing.get('title', ''),
                duration_seconds=existing.get('duration_seconds', 0)
            )

            # Check if analysis failed
            if insights.get('analysis_status') == 'failed':
                logger.error(f"Insights analysis failed: {insights.get('content_summary', 'Unknown error')}")
                return jsonify({'error': f'Analysis failed: {insights.get("content_summary", "Unknown error")}'}), 500

            logger.info(f"Insights generated successfully for {video_id}")
            current_app.local_db.update_content_insights(video_id, insights)

            return jsonify({
                'status': 'completed',
                'message': 'Insights generated from existing data',
                'video_id': video_id
            })
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({'error': f'Failed to generate insights: {str(e)}'}), 500

    # Pre-check: Verify required dependencies for download
    groq_key = os.getenv('GROQ_API_KEY')
    openai_key = os.getenv('OPENAI_API_KEY')

    if generate_transcript and not groq_key and not openai_key:
        return jsonify({'error': 'No transcription API key configured (GROQ_API_KEY or OPENAI_API_KEY)'}), 500

    if analyze_emotions and not os.getenv('HUME_API_KEY'):
        return jsonify({'error': 'HUME_API_KEY not configured but emotion analysis is enabled'}), 500

    # Check if we have at least one download method available (RapidAPI or yt-dlp)
    rapidapi_key = os.getenv('RAPIDAPI_KEYS') or os.getenv('RAPIDAPI_KEY')
    has_ytdlp = False
    try:
        import yt_dlp
        has_ytdlp = True
    except ImportError:
        pass

    if not rapidapi_key and not has_ytdlp:
        return jsonify({'error': 'No download method available. Set RAPIDAPI_KEY or install yt-dlp.'}), 500

    from app.services.transcription_service import TranscriptionService
    import threading

    # Log transcription start
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_start_transcription(email, video_id, {
            'transcript': generate_transcript,
            'emotions': analyze_emotions,
            'frames': analyze_frames,
            'insights': generate_insights
        })

    cache.set(f'transcribing_{video_id}', True, timeout=900)
    cache.set(f'transcribe_progress_{video_id}', {'step': 'download', 'progress': 0, 'message': 'Starting...'}, timeout=900)

    # Get video info and add to transcribing list (for status polling without BigQuery)
    video_info = {'video_id': video_id, 'title': '', 'channel_code': ''}
    try:
        analysis = current_app.bigquery.get_latest_analysis(video_id)
        if analysis and analysis.video:
            video_info['title'] = analysis.video.title
            video_info['channel_code'] = analysis.video.channel_code
    except:
        pass

    transcribing_list = cache.get('transcribing_videos_list') or []
    # Remove if already in list, then add fresh
    transcribing_list = [v for v in transcribing_list if v.get('video_id') != video_id]
    transcribing_list.append(video_info)
    cache.set('transcribing_videos_list', transcribing_list, timeout=1000)

    app = current_app._get_current_object()

    def run_transcription():
        def progress_callback(step, progress, message):
            with app.app_context():
                cache.set(f'transcribe_progress_{video_id}', {
                    'step': step,
                    'progress': progress,
                    'message': message
                }, timeout=900)

        try:
            with app.app_context():
                service = TranscriptionService()

                # Run transcription with selected options
                result = service.transcribe_video(
                    video_id,
                    frame_interval=frame_interval if analyze_frames else None,
                    store_segments=store_segments,
                    analyze_frames=analyze_frames,
                    analyze_emotions=analyze_emotions,
                    generate_transcript=generate_transcript,
                    generate_insights=generate_insights,
                    existing_data=existing,  # Pass existing data for merging
                    progress_callback=progress_callback
                )

                if result:
                    progress_callback('save', 98, 'Storing in database...')

                    # Merge with existing data for fields not regenerated
                    final_transcript = result.get('transcript') or (existing.get('transcript') if existing else None)
                    final_emotions = result.get('emotions') or (existing.get('emotions') if existing else None)
                    final_frames = result.get('frame_analysis') or (existing.get('frame_analysis') if existing else None)
                    final_insights = result.get('content_insights') or (existing.get('content_insights') if existing else None)

                    app.local_db.store_transcript(
                        video_id=result['video_id'],
                        title=result.get('title') or (existing.get('title') if existing else ''),
                        channel=result.get('channel') or (existing.get('channel') if existing else ''),
                        duration_seconds=result.get('duration_seconds') or (existing.get('duration_seconds') if existing else 0),
                        transcript=final_transcript or '',
                        word_count=result.get('word_count') or (existing.get('word_count') if existing else 0),
                        provider=result.get('provider') or (existing.get('provider') if existing else 'unknown'),
                        segments=result.get('segments'),
                        frames=result.get('frame_timestamps'),
                        frame_interval=result.get('frame_interval_seconds'),
                        frame_analysis=final_frames,
                        emotions=final_emotions,
                        description=result.get('description') or (existing.get('description') if existing else None),
                        content_insights=final_insights
                    )
                    progress_callback('completed', 100, 'Complete!')
                else:
                    with app.app_context():
                        cache.set(f'transcribe_error_{video_id}', 'Processing failed. Check server logs.', timeout=300)
        except Exception as e:
            import traceback
            error_msg = str(e)
            logger.error(f"Transcription error for {video_id}: {error_msg}")
            logger.error(traceback.format_exc())
            with app.app_context():
                cache.set(f'transcribe_error_{video_id}', error_msg, timeout=300)
        finally:
            with app.app_context():
                cache.delete(f'transcribing_{video_id}')
                cache.delete(f'transcribe_progress_{video_id}')
                cache.delete(f'video_detail_{video_id}')  # Invalidate video cache

    thread = threading.Thread(target=run_transcription)
    thread.daemon = True
    thread.start()

    features = []
    if generate_transcript:
        features.append("transcript")
    if analyze_emotions:
        features.append("emotions")
    if analyze_frames:
        features.append("frames")
    if generate_insights:
        features.append("insights")

    return jsonify({
        'status': 'started',
        'message': f'Processing: {", ".join(features)}',
        'video_id': video_id
    })


@bp.route('/transcript/<video_id>')
@login_required
def get_transcript(video_id):
    """Get transcript for a video."""
    # Check if transcribing
    is_transcribing = cache.get(f'transcribing_{video_id}') or False

    transcript = current_app.local_db.get_transcript(video_id)

    if not transcript:
        return jsonify({
            'exists': False,
            'is_transcribing': is_transcribing
        })

    return jsonify({
        'exists': True,
        'is_transcribing': False,
        'transcript': transcript
    })


@bp.route('/transcript/<video_id>/history')
@login_required
def get_transcript_history(video_id):
    """Get historical transcript data for a video."""
    limit = request.args.get('limit', 10, type=int)
    history = current_app.local_db.get_transcript_history(video_id, limit)

    return jsonify({
        'video_id': video_id,
        'history': history,
        'count': len(history)
    })


@bp.route('/transcript/history/<int:history_id>')
@login_required
def get_transcript_history_detail(history_id):
    """Get full details of a historical transcript entry."""
    detail = current_app.local_db.get_transcript_history_detail(history_id)

    if not detail:
        return jsonify({'error': 'History entry not found'}), 404

    return jsonify(detail)


@bp.route('/transcript/<video_id>', methods=['DELETE'])
@login_required
def delete_transcript(video_id):
    """Delete transcript to allow re-transcription."""
    success = current_app.local_db.delete_transcript(video_id)

    if success:
        return jsonify({'status': 'deleted', 'video_id': video_id})
    else:
        return jsonify({'error': 'Failed to delete transcript'}), 500


@bp.route('/transcripts')
@login_required
def list_transcripts():
    """Get all stored transcripts (history)."""
    limit = request.args.get('limit', 50, type=int)
    transcripts = current_app.local_db.get_all_transcripts(limit)

    return jsonify({
        'transcripts': transcripts,
        'count': len(transcripts)
    })


@bp.route('/transcribe/status/<video_id>')
@login_required
def get_transcribe_status(video_id):
    """
    Get the current status and progress of a transcription job.

    Args:
        video_id: YouTube video ID

    Returns:
        {
            "status": "processing|completed|error|not_found",
            "step": "download|transcribe|emotions|frames|save",
            "progress": 0-100,
            "error": "error message if status is error"
        }
    """
    # Check for errors first
    error_msg = cache.get(f'transcribe_error_{video_id}')
    if error_msg:
        cache.delete(f'transcribe_error_{video_id}')  # Clear after reading
        return jsonify({
            'status': 'error',
            'step': None,
            'progress': 0,
            'error': error_msg
        })

    # Check if transcribing
    is_transcribing = cache.get(f'transcribing_{video_id}')

    if not is_transcribing:
        # Check if transcript exists (completed)
        transcript = current_app.local_db.get_transcript(video_id)
        if transcript:
            return jsonify({
                'status': 'completed',
                'step': 'completed',
                'progress': 100
            })
        else:
            return jsonify({
                'status': 'not_found',
                'step': None,
                'progress': 0
            })

    # Get progress details from cache
    progress_data = cache.get(f'transcribe_progress_{video_id}') or {}

    return jsonify({
        'status': 'processing',
        'step': progress_data.get('step', 'download'),
        'progress': progress_data.get('progress', 5),
        'message': progress_data.get('message', 'Processing...')
    })


@bp.route('/transcribe/status')
@login_required
def get_all_transcribe_status():
    """
    Check if any transcriptions are currently running.
    Uses a separate cache key to track transcribing videos (no BigQuery calls).

    Returns:
        {
            "is_transcribing": true/false,
            "videos_transcribing": [
                {"video_id": "...", "title": "...", "progress": 50, "message": "..."},
                ...
            ]
        }
    """
    # Get list of transcribing videos from cache (no BigQuery!)
    transcribing_list = cache.get('transcribing_videos_list') or []

    # Filter to only those still marked as transcribing
    videos_transcribing = []
    for video_info in transcribing_list:
        video_id = video_info.get('video_id')
        if cache.get(f'transcribing_{video_id}'):
            progress_data = cache.get(f'transcribe_progress_{video_id}') or {}
            videos_transcribing.append({
                'video_id': video_id,
                'title': video_info.get('title', ''),
                'channel_code': video_info.get('channel_code', ''),
                'progress': progress_data.get('progress', 0),
                'step': progress_data.get('step', 'download'),
                'message': progress_data.get('message', 'Processing...')
            })

    return jsonify({
        'is_transcribing': len(videos_transcribing) > 0,
        'videos_transcribing': videos_transcribing,
        'count': len(videos_transcribing)
    })


@bp.route('/regenerate-insights/<video_id>', methods=['POST'])
@login_required
def regenerate_insights(video_id):
    """
    Regenerate AI Content Insights using existing transcript data.

    Request body:
        {
            "use_transcript": true,    // Include transcript text in analysis
            "use_emotions": true,      // Include emotion data (if available)
            "use_frames": true         // Include frame analysis data (if available)
        }

    Returns:
        {"status": "success|error", "message": "...", "insights": {...}}
    """
    import os

    # Get existing transcript data
    transcript_data = current_app.local_db.get_transcript(video_id)
    if not transcript_data:
        return jsonify({
            'status': 'error',
            'message': 'No transcript data found for this video. Run Transcribe & Analyze first.'
        }), 404

    # Parse request options
    data = request.get_json() or {}
    use_transcript = data.get('use_transcript', True)
    use_emotions = data.get('use_emotions', True)
    use_frames = data.get('use_frames', True)

    # Validate at least one source is selected
    if not use_transcript and not use_emotions and not use_frames:
        return jsonify({
            'status': 'error',
            'message': 'At least one data source must be selected.'
        }), 400

    # Check API key
    anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
    if not anthropic_api_key:
        return jsonify({
            'status': 'error',
            'message': 'ANTHROPIC_API_KEY not configured.'
        }), 500

    import logging
    logger = logging.getLogger(__name__)

    try:
        from app.services.multimodal_analyzer import MultimodalAnalyzer

        # Prepare data based on selected options
        transcript_text = transcript_data.get('transcript') if use_transcript else None
        emotions = transcript_data.get('emotions') if use_emotions else None
        frame_analysis = transcript_data.get('frame_analysis') if use_frames else None

        logger.info(f"Regenerating insights for {video_id}: transcript={bool(transcript_text)}, emotions={bool(emotions)}, frames={bool(frame_analysis)}")

        # Check if selected sources have data
        sources_with_data = []
        if use_transcript and transcript_text:
            sources_with_data.append('transcript')
        if use_emotions and emotions:
            sources_with_data.append('emotions')
        if use_frames and frame_analysis:
            sources_with_data.append('frames')

        if not sources_with_data:
            return jsonify({
                'status': 'error',
                'message': 'Selected data sources have no data. Try selecting different sources.'
            }), 400

        logger.info(f"Running multimodal analysis with sources: {sources_with_data}")

        # Run multimodal analysis
        analyzer = MultimodalAnalyzer(anthropic_api_key)
        insights = analyzer.analyze_content(
            transcript=transcript_text or '',
            emotions=emotions,
            frame_analysis=frame_analysis,
            title=transcript_data.get('title', ''),
            duration_seconds=transcript_data.get('duration_seconds', 0)
        )

        logger.info(f"Analysis complete, status: {insights.get('analysis_status', 'unknown')}")

        # Check if analysis failed
        if insights.get('analysis_status') == 'failed':
            return jsonify({
                'status': 'error',
                'message': insights.get('content_summary', 'Analysis failed')
            }), 500

        # Store the updated insights
        success = current_app.local_db.update_content_insights(video_id, insights)

        if success:
            return jsonify({
                'status': 'success',
                'message': f'Insights regenerated using: {", ".join(sources_with_data)}',
                'insights': insights
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save insights to database.'
            }), 500

    except Exception as e:
        import traceback
        logger.error(f"Error regenerating insights for {video_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': f'Analysis failed: {str(e)[:200]}'
        }), 500


# ==================== YOUTUBE COMMENTS ====================

@bp.route('/comments/fetch', methods=['POST'])
@login_required
def fetch_comments():
    """
    Fetch YouTube comments for one or more videos and store in database.

    Request body:
        {
            "video_ids": ["id1", "id2", ...],  // List of video IDs
            "max_per_video": 50                 // Optional, default 50
        }

    Returns:
        {"status": "started", "video_count": N}
    """
    data = request.get_json()
    video_ids = data.get('video_ids', [])
    max_per_video = data.get('max_per_video', 50)

    if not video_ids:
        return jsonify({'error': 'video_ids is required'}), 400

    if not current_app.youtube_comments.youtube:
        return jsonify({'error': 'YouTube API not configured. Set YOUTUBE_API_KEY in .env.web'}), 500

    # Log activity
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(
            email, 'Fetch Comments',
            f'videos={len(video_ids)}, max={max_per_video}'
        )

    # Run in background thread for batch operations
    if len(video_ids) > 1:
        import threading

        cache.set('fetching_comments', True, timeout=600)
        cache.set('comments_progress', {
            'total': len(video_ids),
            'processed': 0,
            'current': video_ids[0],
            'results': {}
        }, timeout=600)

        app = current_app._get_current_object()

        def run_batch():
            try:
                with app.app_context():
                    results = {}
                    for i, vid in enumerate(video_ids):
                        cache.set('comments_progress', {
                            'total': len(video_ids),
                            'processed': i,
                            'current': vid,
                            'results': results
                        }, timeout=600)

                        count = app.youtube_comments.fetch_and_store(vid, max_per_video)
                        results[vid] = count

                    cache.set('comments_progress', {
                        'total': len(video_ids),
                        'processed': len(video_ids),
                        'current': None,
                        'results': results,
                        'done': True
                    }, timeout=300)
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Batch comments error: {e}")
            finally:
                with app.app_context():
                    cache.delete('fetching_comments')

        thread = threading.Thread(target=run_batch)
        thread.daemon = True
        thread.start()

        return jsonify({
            'status': 'started',
            'video_count': len(video_ids),
            'message': f'Fetching comments for {len(video_ids)} videos in background'
        })
    else:
        # Single video - do it synchronously
        count = current_app.youtube_comments.fetch_and_store(video_ids[0], max_per_video)
        return jsonify({
            'status': 'completed',
            'video_id': video_ids[0],
            'comments_stored': count
        })


@bp.route('/comments/status')
@login_required
def comments_fetch_status():
    """Get status of batch comment fetching."""
    progress = cache.get('comments_progress')
    if not progress:
        return jsonify({'status': 'idle'})

    if progress.get('done'):
        return jsonify({
            'status': 'completed',
            'total': progress['total'],
            'processed': progress['processed'],
            'results': progress['results']
        })

    return jsonify({
        'status': 'processing',
        'total': progress['total'],
        'processed': progress['processed'],
        'current': progress.get('current')
    })


@bp.route('/comments/<video_id>')
@login_required
def get_comments(video_id):
    """Get stored comments for a video."""
    comments = current_app.local_db.get_comments(video_id)
    pinned = next((c for c in comments if c.get('is_pinned')), None)

    return jsonify({
        'video_id': video_id,
        'comments': comments,
        'count': len(comments),
        'pinned_comment': pinned,
        'has_comments': len(comments) > 0
    })


# ==================== CTA SCORING ====================

@bp.route('/cta-score', methods=['POST'])
@login_required
def score_cta():
    """
    Score CTA and description quality for one or more videos using Claude.

    Request body:
        {
            "video_ids": ["id1", "id2"],  // Videos to score (from audit data)
            "audit_data": [...]           // The audit row data (title, description, silo, etc.)
        }

    Returns:
        {"status": "started|completed", "scores": {...}}
    """
    import threading

    data = request.get_json()
    videos = data.get('videos', [])

    if not videos:
        return jsonify({'error': 'videos array is required'}), 400

    if not current_app.config.get('ANTHROPIC_API_KEY'):
        return jsonify({'error': 'ANTHROPIC_API_KEY not configured'}), 500

    preferred_brands = current_app.config.get('PREFERRED_BRANDS', {})
    app = current_app._get_current_object()

    # For small batches (<=3), do synchronously
    if len(videos) <= 3:
        from app.services.conversion_analyzer import ConversionAnalyzer
        analyzer = ConversionAnalyzer(api_key=app.config['ANTHROPIC_API_KEY'])
        scores = {}

        for v in videos:
            video_id = v.get('video_id')
            silo = (v.get('silo') or '').strip()
            preferred = preferred_brands.get(silo, preferred_brands.get(silo.lower(), ''))

            result = analyzer.score_cta_and_description(
                title=v.get('title', ''),
                description=v.get('description', ''),
                silo=silo,
                keyword=v.get('keyword', ''),
                preferred_brand=preferred or None,
                desc_brand=v.get('desc_brand', ''),
                comment_brand=v.get('comment_brand', '')
            )

            # Store in local DB
            app.local_db.store_cta_audit_score(
                video_id=video_id,
                cta_score=result['cta_score'],
                description_score=result['description_score'],
                base_score=result['base_score'],
                has_preferred_brand=result['has_preferred_brand'],
                preferred_brand=preferred or '',
                adjusted_score=result['adjusted_score'],
                scoring_reasoning=result['reasoning']
            )

            scores[video_id] = result

        return jsonify({'status': 'completed', 'scores': scores})

    # For larger batches, run in background
    cache.set('cta_scoring_progress', {
        'total': len(videos), 'processed': 0, 'current': None
    }, timeout=600)

    def run_batch_scoring():
        try:
            with app.app_context():
                from app.services.conversion_analyzer import ConversionAnalyzer
                analyzer = ConversionAnalyzer(api_key=app.config['ANTHROPIC_API_KEY'])

                for i, v in enumerate(videos):
                    video_id = v.get('video_id')
                    cache.set('cta_scoring_progress', {
                        'total': len(videos), 'processed': i, 'current': video_id
                    }, timeout=600)

                    silo = (v.get('silo') or '').strip()
                    preferred = preferred_brands.get(silo, preferred_brands.get(silo.lower(), ''))

                    result = analyzer.score_cta_and_description(
                        title=v.get('title', ''),
                        description=v.get('description', ''),
                        silo=silo,
                        keyword=v.get('keyword', ''),
                        preferred_brand=preferred or None,
                        desc_brand=v.get('desc_brand', ''),
                        comment_brand=v.get('comment_brand', '')
                    )

                    app.local_db.store_cta_audit_score(
                        video_id=video_id,
                        cta_score=result['cta_score'],
                        description_score=result['description_score'],
                        base_score=result['base_score'],
                        has_preferred_brand=result['has_preferred_brand'],
                        preferred_brand=preferred or '',
                        adjusted_score=result['adjusted_score'],
                        scoring_reasoning=result['reasoning']
                    )

                cache.set('cta_scoring_progress', {
                    'total': len(videos), 'processed': len(videos),
                    'current': None, 'done': True
                }, timeout=300)
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Batch CTA scoring error: {e}")
        finally:
            with app.app_context():
                cache.delete('cta_scoring_active')

    cache.set('cta_scoring_active', True, timeout=600)
    thread = threading.Thread(target=run_batch_scoring)
    thread.daemon = True
    thread.start()

    return jsonify({
        'status': 'started',
        'message': f'Scoring {len(videos)} videos in background'
    })


@bp.route('/cta-score/status')
@login_required
def cta_score_status():
    """Get status of batch CTA scoring."""
    progress = cache.get('cta_scoring_progress')
    if not progress:
        return jsonify({'status': 'idle'})

    if progress.get('done'):
        return jsonify({
            'status': 'completed',
            'total': progress['total'],
            'processed': progress['processed']
        })

    return jsonify({
        'status': 'processing',
        'total': progress['total'],
        'processed': progress['processed'],
        'current': progress.get('current')
    })


@bp.route('/preferred-brands')
@login_required
def get_preferred_brands():
    """Get the preferred brand mapping per silo."""
    return jsonify(current_app.config.get('PREFERRED_BRANDS', {}))


@bp.route('/preferred-brands', methods=['POST'])
@login_required
def update_preferred_brands():
    """Update preferred brand mapping.

    Request body:
        {"identitytheft": "Aura", "database": "Aura", "PC": "Aura", ...}
    """
    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({'error': 'Expected a JSON object mapping silo -> brand'}), 400

    current_app.config['PREFERRED_BRANDS'] = data
    return jsonify({'status': 'updated', 'brands': data})


# ==================== SCRIPT SCORING (Approved Brands / Partners / Scores) ====================

@bp.route('/approved-brands')
@login_required
def get_approved_brands():
    """Get all approved brand mappings (silo -> brand)."""
    brands = current_app.local_db.get_approved_brands()
    return jsonify({'brands': brands, 'count': len(brands)})


@bp.route('/approved-brands', methods=['POST'])
@login_required
def save_approved_brand():
    """Add or update an approved brand for a silo.

    Request body:
        {"silo": "identitytheft", "primary_brand": "Aura", "secondary_brand": "LifeLock", "notes": ""}
    """
    data = request.get_json()
    silo = data.get('silo')
    primary_brand = data.get('primary_brand')

    if not silo or not primary_brand:
        return jsonify({'error': 'silo and primary_brand are required'}), 400

    success = current_app.local_db.store_approved_brand(
        silo=silo,
        primary_brand=primary_brand,
        secondary_brand=data.get('secondary_brand'),
        notes=data.get('notes')
    )
    if success:
        return jsonify({'status': 'saved', 'silo': silo, 'primary_brand': primary_brand})
    return jsonify({'error': 'Failed to save'}), 500


@bp.route('/approved-brands/<silo>', methods=['DELETE'])
@login_required
def delete_approved_brand(silo):
    """Delete approved brand for a silo."""
    success = current_app.local_db.delete_approved_brand(silo)
    if success:
        return jsonify({'status': 'deleted', 'silo': silo})
    return jsonify({'error': 'Failed to delete'}), 500


@bp.route('/partner-list')
@login_required
def get_partner_list():
    """Get all partner brands."""
    active_only = request.args.get('active_only', 'true').lower() == 'true'
    partners = current_app.local_db.get_partner_list(active_only=active_only)
    return jsonify({'partners': partners, 'count': len(partners)})


@bp.route('/partner-list', methods=['POST'])
@login_required
def save_partner():
    """Add or update a partner brand.

    Request body:
        {"brand_name": "Aura", "silo": "identitytheft", "is_active": true, "notes": ""}
    """
    data = request.get_json()
    brand_name = data.get('brand_name')

    if not brand_name:
        return jsonify({'error': 'brand_name is required'}), 400

    success = current_app.local_db.store_partner(
        brand_name=brand_name,
        silo=data.get('silo'),
        is_active=data.get('is_active', True),
        notes=data.get('notes')
    )
    if success:
        return jsonify({'status': 'saved', 'brand_name': brand_name})
    return jsonify({'error': 'Failed to save'}), 500


@bp.route('/partner-list/<brand_name>', methods=['DELETE'])
@login_required
def delete_partner(brand_name):
    """Delete a partner brand."""
    success = current_app.local_db.delete_partner(brand_name)
    if success:
        return jsonify({'status': 'deleted', 'brand_name': brand_name})
    return jsonify({'error': 'Failed to delete'}), 500


@bp.route('/script-scores')
@login_required
def get_script_scores():
    """Get all script scores (for library view)."""
    scores = current_app.local_db.get_all_script_scores()
    return jsonify({'scores': scores, 'count': len(scores)})


@bp.route('/script-scores/<video_id>')
@login_required
def get_script_score(video_id):
    """Get script score for a specific video."""
    score = current_app.local_db.get_script_score(video_id)
    if not score:
        return jsonify({'error': 'No script score found'}), 404
    return jsonify(score)


# ==================== CLIENT-SIDE ACTIVITY TRACKING ====================

@bp.route('/log-activity', methods=['POST'])
@login_required
def log_client_activity():
    """
    Log client-side user activity.

    Request body:
        {
            "action": "Click Watch on YouTube",
            "details": "video_id=abc123"
        }

    Returns:
        {"status": "logged"}
    """
    data = request.get_json()
    action = data.get('action', 'Unknown Action')
    details = data.get('details', '')

    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(email, action, details)

    return jsonify({'status': 'logged'})
