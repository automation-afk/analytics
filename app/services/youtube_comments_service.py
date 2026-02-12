"""YouTube Comments Service - fetches and parses video comments via YouTube Data API v3."""
import json
import logging
import os
import re
from datetime import datetime
from typing import List, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# URL pattern to extract links from comment text
URL_PATTERN = re.compile(
    r'https?://[^\s<>"\')\]]+|'
    r'(?:www\.)[^\s<>"\')\]]+',
    re.IGNORECASE
)

# Affiliate URL indicators (from affiliate_recommender.py patterns)
AFFILIATE_URL_PATTERNS = [
    'amzn.to', '/ref=', '?tag=', 'affiliate', 'aff=', 'referral',
    '/go/', 'geni.us', 'click.linksynergy.com', 'shareasale.com',
    'cj.com', 'avantlink.com', 'pxf.io', 'dpbolvw.net', 'impact.com',
    'sjv.io', 'tkqlhce.com', 'jdoqocy.com', 'kqzyfj.com',
    'bit.ly/', 'tinyurl.com/', 'ow.ly/', 'buff.ly/', 'rebrand.ly/',
    'shorturl.at/', 'cutt.ly/',
]

# Subdomain patterns that suggest affiliate/promo links
AFFILIATE_SUBDOMAIN_PATTERNS = [
    r'deal\.', r'offer\.', r'try\.', r'get\.', r'go\.', r'promo\.',
]


class YouTubeCommentsService:
    """Service for fetching and analyzing YouTube video comments."""

    def __init__(self, api_key: str = None, credentials_path: str = None,
                 local_db=None):
        """Initialize with YouTube Data API key or service account credentials.

        Args:
            api_key: YouTube Data API v3 key (optional)
            credentials_path: Path to service account JSON file (optional)
            local_db: LocalDBService instance for storing comments
        """
        self.local_db = local_db
        self.youtube = None
        self._known_brands = set()

        try:
            if credentials_path and os.path.exists(credentials_path):
                # Service account credentials (preferred)
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/youtube.force-ssl']
                )
                self.youtube = build('youtube', 'v3', credentials=credentials)
                logger.info(f"YouTube Comments Service initialized with service account: {credentials_path}")
            elif api_key:
                # API key fallback
                self.youtube = build('youtube', 'v3', developerKey=api_key)
                logger.info("YouTube Comments Service initialized with API key")
            else:
                logger.warning("No YouTube credentials provided - comments service disabled")
        except Exception as e:
            logger.error(f"Failed to initialize YouTube API client: {e}")

    def set_known_brands(self, brands: list):
        """Set known affiliate brand names for detection in comments.

        Args:
            brands: List of brand name strings (e.g., ['Aura', 'Norton', 'Incogni'])
        """
        self._known_brands = {b.lower() for b in brands if b}

    def fetch_comments(self, video_id: str, max_results: int = 100) -> List[Dict]:
        """Fetch comments for a video from YouTube Data API.

        Args:
            video_id: YouTube video ID
            max_results: Maximum number of comment threads to fetch

        Returns:
            List of parsed comment dicts
        """
        if not self.youtube:
            logger.warning("YouTube API not initialized")
            return []

        comments = []
        try:
            # Fetch with relevance ordering - pinned comment appears first
            request = self.youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                order='relevance',
                maxResults=min(max_results, 100),
                textFormat='plainText'
            )

            response = request.execute()
            items = response.get('items', [])

            # Get the video's channel ID to identify channel owner comments
            channel_id = self._get_video_channel_id(video_id)

            for i, item in enumerate(items):
                snippet = item['snippet']['topLevelComment']['snippet']
                comment = self._parse_comment(item, snippet, channel_id, is_first=(i == 0))
                comments.append(comment)

            # Handle pagination if needed
            while 'nextPageToken' in response and len(comments) < max_results:
                request = self.youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    order='relevance',
                    maxResults=min(max_results - len(comments), 100),
                    pageToken=response['nextPageToken'],
                    textFormat='plainText'
                )
                response = request.execute()
                for item in response.get('items', []):
                    snippet = item['snippet']['topLevelComment']['snippet']
                    comment = self._parse_comment(item, snippet, channel_id, is_first=False)
                    comments.append(comment)

            logger.info(f"Fetched {len(comments)} comments for video {video_id}")

        except HttpError as e:
            if e.resp.status == 403:
                logger.error(f"Comments disabled or API quota exceeded for {video_id}: {e}")
            elif e.resp.status == 404:
                logger.error(f"Video not found: {video_id}")
            else:
                logger.error(f"YouTube API error for {video_id}: {e}")
        except Exception as e:
            logger.error(f"Error fetching comments for {video_id}: {e}")

        return comments

    def fetch_and_store(self, video_id: str, max_results: int = 100) -> int:
        """Fetch comments from YouTube and store them in the database.

        Args:
            video_id: YouTube video ID
            max_results: Maximum comments to fetch

        Returns:
            Number of comments stored
        """
        comments = self.fetch_comments(video_id, max_results)
        if not comments:
            return 0

        if self.local_db:
            # Delete old comments for this video before storing new ones
            self.local_db.delete_comments(video_id)
            return self.local_db.store_comments(comments)

        return len(comments)

    def fetch_and_store_batch(self, video_ids: List[str], max_per_video: int = 50) -> Dict:
        """Fetch and store comments for multiple videos.

        Args:
            video_ids: List of video IDs
            max_per_video: Max comments per video

        Returns:
            Dict with results: {video_id: count_stored, ...}
        """
        results = {}
        for vid in video_ids:
            try:
                count = self.fetch_and_store(vid, max_per_video)
                results[vid] = count
                logger.info(f"Stored {count} comments for {vid}")
            except Exception as e:
                logger.error(f"Failed to process comments for {vid}: {e}")
                results[vid] = 0
        return results

    def _get_video_channel_id(self, video_id: str) -> Optional[str]:
        """Get the channel ID that owns a video."""
        try:
            response = self.youtube.videos().list(
                part='snippet',
                id=video_id
            ).execute()

            items = response.get('items', [])
            if items:
                return items[0]['snippet']['channelId']
        except Exception as e:
            logger.error(f"Error getting channel ID for {video_id}: {e}")
        return None

    def _parse_comment(self, item: dict, snippet: dict, channel_id: str,
                       is_first: bool = False) -> Dict:
        """Parse a comment thread item into a structured dict.

        Args:
            item: Full commentThread item from API
            snippet: The topLevelComment.snippet
            channel_id: The video owner's channel ID
            is_first: Whether this is the first comment (potential pinned)
        """
        author_channel_id = snippet.get('authorChannelId', {}).get('value', '')
        is_channel_owner = (author_channel_id == channel_id) if channel_id else False
        comment_text = snippet.get('textDisplay', '')

        # Pinned comment heuristic: first comment by the channel owner
        # YouTube API sorts pinned comment first when order=relevance
        is_pinned = is_first and is_channel_owner

        # Extract URLs from comment
        links = self._extract_links(comment_text)

        # Detect brands from URLs and text
        brands = self._detect_brands(comment_text, links)

        return {
            'video_id': snippet.get('videoId', ''),
            'comment_id': item['snippet']['topLevelComment']['id'],
            'comment_text': comment_text,
            'author_name': snippet.get('authorDisplayName', ''),
            'author_channel_id': author_channel_id,
            'like_count': snippet.get('likeCount', 0),
            'is_pinned': is_pinned,
            'is_channel_owner': is_channel_owner,
            'published_at': snippet.get('publishedAt', ''),
            'links_found': links,
            'brands_detected': brands,
        }

    def _extract_links(self, text: str) -> List[str]:
        """Extract URLs from comment text."""
        if not text:
            return []
        matches = URL_PATTERN.findall(text)
        # Clean trailing punctuation
        cleaned = []
        for url in matches:
            url = url.rstrip('.,;:!?)]}')
            if len(url) > 10:
                cleaned.append(url)
        return cleaned

    def _detect_brands(self, text: str, links: List[str]) -> List[str]:
        """Detect affiliate/brand names from comment text and links.

        Uses both URL pattern matching and text-based brand name search.
        """
        brands = set()
        text_lower = text.lower() if text else ''

        # Check links for affiliate patterns and extract brand from URL
        for link in links:
            link_lower = link.lower()

            # Check if it's an affiliate link
            is_affiliate = any(pattern in link_lower for pattern in AFFILIATE_URL_PATTERNS)
            is_subdomain_affiliate = any(
                re.search(pattern, link_lower) for pattern in AFFILIATE_SUBDOMAIN_PATTERNS
            )

            if is_affiliate or is_subdomain_affiliate:
                brand = self._extract_brand_from_url(link)
                if brand:
                    brands.add(brand)

        # Check text for known brand names
        for brand in self._known_brands:
            if brand in text_lower:
                brands.add(brand.title())

        return sorted(brands)

    def _extract_brand_from_url(self, url: str) -> Optional[str]:
        """Extract brand name from a URL.

        Examples:
            https://try.aura.com/xyz -> Aura
            https://deal.incogni.io/xyz -> Incogni
            https://go.nordvpn.net/xyz -> NordVPN
            https://bit.ly/xyz -> None (can't determine from shortener)
        """
        try:
            # Remove protocol
            clean = re.sub(r'^https?://', '', url.lower())

            # Check for subdomain patterns like try.aura.com, deal.incogni.io
            subdomain_match = re.match(
                r'(?:deal|offer|try|get|go|promo)\.([a-z0-9-]+)\.',
                clean
            )
            if subdomain_match:
                return subdomain_match.group(1).title()

            # Skip pure shortener domains
            shorteners = ['bit.ly', 'tinyurl.com', 'ow.ly', 'buff.ly',
                          'rebrand.ly', 'shorturl.at', 'cutt.ly', 'geni.us',
                          'amzn.to']
            for s in shorteners:
                if clean.startswith(s):
                    return None

            # Skip affiliate network domains
            networks = ['click.linksynergy.com', 'shareasale.com', 'avantlink.com',
                        'pxf.io', 'dpbolvw.net', 'impact.com', 'sjv.io',
                        'tkqlhce.com', 'jdoqocy.com', 'kqzyfj.com']
            for n in networks:
                if clean.startswith(n):
                    return None

            # Try to get domain name as brand (e.g., nordvpn.net -> NordVPN)
            domain_match = re.match(r'(?:www\.)?([a-z0-9-]+)\.', clean)
            if domain_match:
                domain = domain_match.group(1)
                # Skip generic domains
                if domain not in ['youtube', 'google', 'facebook', 'twitter',
                                  'instagram', 'reddit', 'amazon', 'ebay']:
                    return domain.title()

        except Exception:
            pass
        return None
