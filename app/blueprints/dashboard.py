"""Dashboard blueprint - main overview and video list pages."""
import csv
import io
from flask import Blueprint, render_template, request, current_app, redirect, url_for, session, Response, jsonify
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


@bp.route('/dashboard/conversion-audit')
@login_required
def conversion_audit():
    """Conversion audit dashboard - all videos with key conversion metrics."""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    channel_code = request.args.get('channel')
    keyword_search = request.args.get('keyword')
    silo_filter = request.args.get('silo')
    sort_by = request.args.get('sort_by', 'avg_monthly_revenue')
    sort_dir = request.args.get('sort_dir', 'desc')

    # Log activity
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(email, 'View Conversion Audit', f'page={page}')

    offset = (page - 1) * per_page

    # Cache key
    cache_key = f'conv_audit_{page}_{channel_code}_{keyword_search}_{silo_filter}_{sort_by}_{sort_dir}'
    cached_data = cache.get(cache_key)

    if cached_data is not None:
        audit_data = cached_data
    else:
        audit_data = current_app.bigquery.get_conversion_audit_data(
            limit=per_page,
            offset=offset,
            channel_code=channel_code,
            keyword_search=keyword_search,
            silo=silo_filter,
            sort_by=sort_by,
            sort_dir=sort_dir
        )
        cache.set(cache_key, audit_data, timeout=120)

    # Merge YouTube comment brand data and CTA scores from local DB
    if audit_data:
        video_ids = [row['video_id'] for row in audit_data]
        comment_summary = current_app.local_db.get_comments_summary(video_ids)
        cta_scores = current_app.local_db.get_cta_audit_scores(video_ids)
        preferred_brands = current_app.config.get('PREFERRED_BRANDS', {})

        for row in audit_data:
            vid = row['video_id']
            if vid in comment_summary:
                summary = comment_summary[vid]
                # Use YouTube comment brand if BigQuery doesn't have one
                if not row.get('comment_brand') and summary.get('pinned_brand'):
                    row['comment_brand'] = summary['pinned_brand']
                # Add pinned comment text and links for modal display
                row['pinned_text'] = summary.get('pinned_text', '')
                row['pinned_author'] = summary.get('pinned_author', '')
                row['pinned_links'] = summary.get('pinned_links', [])
                row['has_yt_comments'] = True
            else:
                row['pinned_text'] = ''
                row['pinned_author'] = ''
                row['pinned_links'] = []
                row['has_yt_comments'] = False

            # Merge CTA scores
            if vid in cta_scores:
                row['cta_audit'] = cta_scores[vid]
            else:
                row['cta_audit'] = None

            # Add preferred brand for this silo
            silo = (row.get('silo') or '').strip()
            row['preferred_brand'] = preferred_brands.get(silo, preferred_brands.get(silo.lower(), ''))

    # Get filter dropdowns (cached)
    all_channels = cache.get('all_channels')
    if all_channels is None:
        all_channels = current_app.bigquery.get_all_channels()
        cache.set('all_channels', all_channels, timeout=600)

    all_silos = cache.get('all_silos')
    if all_silos is None:
        all_silos = current_app.bigquery.get_all_silos()
        cache.set('all_silos', all_silos, timeout=600)

    has_more = len(audit_data) == per_page

    preferred_brands = current_app.config.get('PREFERRED_BRANDS', {})

    return render_template(
        'dashboard/conversion_audit.html',
        audit_data=audit_data,
        page=page,
        has_more=has_more,
        channel_code=channel_code,
        keyword_search=keyword_search,
        silo_filter=silo_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        all_channels=all_channels,
        all_silos=all_silos,
        preferred_brands=preferred_brands
    )


@bp.route('/dashboard/conversion-audit/export')
@login_required
def conversion_audit_export():
    """Export conversion audit data as CSV."""
    channel_code = request.args.get('channel')
    keyword_search = request.args.get('keyword')
    silo_filter = request.args.get('silo')
    sort_by = request.args.get('sort_by', 'avg_monthly_revenue')
    sort_dir = request.args.get('sort_dir', 'desc')

    # Log activity
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(email, 'Export Conversion Audit')

    audit_data = current_app.bigquery.get_conversion_audit_export(
        channel_code=channel_code,
        keyword_search=keyword_search,
        silo=silo_filter,
        sort_by=sort_by,
        sort_dir=sort_dir
    )

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Video ID', 'Title', 'Channel', 'Keyword', 'Silo',
        'Avg Monthly Revenue (90d)', 'Avg Monthly Views (90d)',
        'EPC (90d)', 'Conversion Rate %', 'Desc CTR %', 'Pinned CTR %',
        'Thumbnail CTR %', 'Rank', 'Desc Brand', 'Comment Brand',
        'Rev by Brand', 'Description'
    ])
    for row in audit_data:
        # Format brand revenue as "Brand: $amount" list
        brand_rev_str = ''
        if row.get('brand_revenue'):
            brand_rev_str = '; '.join(
                f"{br['brand']}: ${br['revenue']:.2f}" for br in row['brand_revenue'][:5]
            )
        writer.writerow([
            row['video_id'],
            row['title'],
            row['channel'],
            row['keyword'],
            row['silo'],
            row['avg_monthly_revenue'],
            row['avg_monthly_views'],
            row.get('epc_90d', 0),
            row['conversion_rate'],
            row['desc_ctr'],
            row['pinned_ctr'],
            row['thumbnail_ctr'],
            row.get('rank') or '',
            row.get('desc_brand') or '',
            row.get('comment_brand') or '',
            brand_rev_str,
            row['description'][:500].replace('\n', ' ').replace('\r', '') if row['description'] else ''
        ])

    csv_content = output.getvalue()
    output.close()

    # Add UTF-8 BOM so Excel reads encoding correctly
    csv_bytes = b'\xef\xbb\xbf' + csv_content.encode('utf-8')

    return Response(
        csv_bytes,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': 'attachment; filename=conversion_audit.csv'}
    )


def compute_optimization_opportunity(revenue_potential, current_revenue, quality_score):
    """(Revenue Potential - Current Revenue) x (100 - Quality Score) / 100"""
    gap = max((revenue_potential or 0) - (current_revenue or 0), 0)
    return round(gap * (100 - (quality_score or 0)) / 100, 2)


@bp.route('/dashboard/script-scores')
@login_required
def script_scores_library():
    """Script Scores Library — sortable table of all scored videos."""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    channel_filter = request.args.get('channel')
    silo_filter = request.args.get('silo')
    keyword_search = request.args.get('keyword')
    gates_filter = request.args.get('gates')  # 'pass' or 'fail'
    sort_by = request.args.get('sort_by', 'optimization_opportunity')
    sort_dir = request.args.get('sort_dir', 'desc')

    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(email, 'View Script Scores Library', f'page={page}')

    # 1. Get all scored videos from local DB
    all_scores = current_app.local_db.get_all_script_scores()
    if not all_scores:
        return render_template(
            'dashboard/script_scores.html',
            scores=[], page=1, has_more=False,
            channel_filter=channel_filter, silo_filter=silo_filter,
            keyword_search=keyword_search, gates_filter=gates_filter,
            sort_by=sort_by, sort_dir=sort_dir,
            all_channels=[], all_silos=[]
        )

    # 2. Get metadata + revenue from BigQuery
    video_ids = [s['video_id'] for s in all_scores]
    cache_key = f'script_scores_meta_{hash(tuple(sorted(video_ids)))}'
    metadata = cache.get(cache_key)
    if metadata is None:
        metadata = current_app.bigquery.get_video_metadata_batch(video_ids)
        cache.set(cache_key, metadata, timeout=600)

    # 3. Merge and compute optimization opportunity
    merged = []
    channels_set = set()
    silos_set = set()
    for s in all_scores:
        vid = s['video_id']
        meta = metadata.get(vid, {})
        row = {**s, **meta}
        # Compute optimization opportunity
        row['optimization_opportunity'] = compute_optimization_opportunity(
            meta.get('revenue_potential', 0),
            meta.get('avg_monthly_revenue', 0),
            s.get('quality_score_total')
        )
        row['revenue_potential'] = meta.get('revenue_potential', 0)
        row['avg_monthly_revenue'] = meta.get('avg_monthly_revenue', 0)
        # Get top fix action item
        items = s.get('action_items', [])
        row['top_fix'] = items[0].get('action', '') if items else ''

        if meta.get('channel'):
            channels_set.add(meta['channel'])
        if meta.get('silo'):
            silos_set.add(meta['silo'])

        merged.append(row)

    # 4. Apply filters
    if channel_filter:
        merged = [r for r in merged if r.get('channel') == channel_filter]
    if silo_filter:
        merged = [r for r in merged if (r.get('silo') or '').lower() == silo_filter.lower()]
    if keyword_search:
        kw = keyword_search.lower()
        merged = [r for r in merged if kw in (r.get('main_keyword') or '').lower()
                  or kw in (r.get('title') or '').lower()]
    if gates_filter == 'pass':
        merged = [r for r in merged if r.get('all_gates_passed')]
    elif gates_filter == 'fail':
        merged = [r for r in merged if not r.get('all_gates_passed')]

    # 5. Sort
    valid_sort_cols = {
        'title', 'channel', 'main_keyword', 'silo',
        'quality_score_total', 'multiplied_score', 'rizz_score',
        'avg_monthly_revenue', 'revenue_potential', 'optimization_opportunity',
        'all_gates_passed'
    }
    if sort_by not in valid_sort_cols:
        sort_by = 'optimization_opportunity'
    reverse = sort_dir == 'desc'
    merged.sort(key=lambda r: (r.get(sort_by) is not None, r.get(sort_by) or 0), reverse=reverse)

    # 6. Paginate
    total = len(merged)
    offset = (page - 1) * per_page
    page_data = merged[offset:offset + per_page]
    has_more = offset + per_page < total

    return render_template(
        'dashboard/script_scores.html',
        scores=page_data,
        page=page,
        has_more=has_more,
        total=total,
        channel_filter=channel_filter,
        silo_filter=silo_filter,
        keyword_search=keyword_search,
        gates_filter=gates_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        all_channels=sorted(channels_set),
        all_silos=sorted(silos_set)
    )


@bp.route('/dashboard/script-scores/trends')
@login_required
def script_scores_trends():
    """Script Scores Trend View — quality score averages over time by channel."""
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_action(email, 'View Script Scores Trends')

    # Get all scores with timestamps
    all_scores = current_app.local_db.get_scores_by_month()
    if not all_scores:
        return render_template('dashboard/script_scores_trends.html', chart_data={})

    # Get channel for each video
    video_ids = list(set(s['video_id'] for s in all_scores))
    cache_key = f'scores_trend_meta_{hash(tuple(sorted(video_ids)))}'
    metadata = cache.get(cache_key)
    if metadata is None:
        metadata = current_app.bigquery.get_video_metadata_batch(video_ids)
        cache.set(cache_key, metadata, timeout=600)

    # Group by channel + month
    from collections import defaultdict
    channel_month_scores = defaultdict(list)
    for s in all_scores:
        meta = metadata.get(s['video_id'], {})
        channel = meta.get('channel', 'Unknown')
        scored_at = s.get('scored_at', '')
        month = scored_at[:7] if scored_at else ''  # "YYYY-MM"
        if month and s.get('quality_score_total') is not None:
            channel_month_scores[(channel, month)].append(s['quality_score_total'])

    # Build chart data: {months: [...], datasets: {channel: [avg, avg, ...]}}
    all_months = sorted(set(k[1] for k in channel_month_scores.keys()))
    all_channels = sorted(set(k[0] for k in channel_month_scores.keys()))

    datasets = {}
    for channel in all_channels:
        datasets[channel] = []
        for month in all_months:
            scores = channel_month_scores.get((channel, month), [])
            avg = round(sum(scores) / len(scores), 1) if scores else None
            datasets[channel].append(avg)

    chart_data = {
        'months': all_months,
        'datasets': datasets
    }

    return render_template('dashboard/script_scores_trends.html', chart_data=chart_data)


@bp.route('/dashboard/debug/link-placements')
@login_required
def debug_link_placements():
    """Debug: show distinct Link_Placement values from Revenue_Metrics."""
    data = current_app.bigquery.get_distinct_link_placements()
    return jsonify(data)
