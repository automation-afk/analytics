"""
Description Analyzer
Analyzes video descriptions for links, CTAs, and optimization opportunities.
"""

import re
from typing import List, Dict, Optional
from anthropic import Anthropic
import json
import logging

logger = logging.getLogger(__name__)


class DescriptionAnalyzer:
    """
    Analyze video descriptions for effectiveness.

    Analyzes:
    - Links (total, affiliate, positioning)
    - Call-to-action presence and effectiveness
    - Link density and placement
    - Description length and structure
    - Keyword usage
    - Optimization opportunities
    """

    def __init__(self, anthropic_api_key: Optional[str] = None, model: str = "claude-sonnet-4-20250514"):
        """
        Initialize description analyzer.

        Args:
            anthropic_api_key: Optional Anthropic API key for AI analysis
            model: Claude model to use for AI analysis
        """
        self.anthropic_api_key = anthropic_api_key
        self.model = model

        if anthropic_api_key:
            self.client = Anthropic(api_key=anthropic_api_key)
            logger.info("Description analyzer initialized with AI capabilities")
        else:
            self.client = None
            logger.info("Description analyzer initialized (basic analysis only)")

    def analyze(self, description: str, title: str = "") -> Dict:
        """
        Comprehensive description analysis.

        Args:
            description: Video description text
            title: Video title (optional, for context)

        Returns:
            Dictionary with analysis results
        """
        logger.info("Analyzing video description...")

        # Basic analysis (no API needed)
        links = self.extract_links(description)
        link_metrics = self.calculate_link_metrics(description, links)
        structure = self.analyze_structure(description)
        cta_analysis = self.analyze_cta(description)

        result = {
            'description_length': len(description),
            'word_count': len(description.split()),
            'line_count': len(description.split('\n')),
            'total_links': len(links),
            'links': links,
            'link_density': link_metrics['link_density'],
            'link_positioning_score': link_metrics['positioning_score'],
            'has_clear_cta': cta_analysis['has_clear_cta'],
            'cta_count': cta_analysis['cta_count'],
            'cta_examples': cta_analysis['cta_examples'],
            'structure': structure,
            'has_timestamps': self._has_timestamps(description),
            'has_social_links': self._has_social_links(description),
            'has_hashtags': self._has_hashtags(description)
        }

        # AI-powered analysis (if API key provided)
        if self.client:
            ai_analysis = self._ai_analyze_description(description, title)
            if ai_analysis:
                result.update(ai_analysis)

        logger.info(f"Description analysis complete: {result['total_links']} links, "
                   f"{result['word_count']} words")

        return result

    def extract_links(self, description: str) -> List[Dict]:
        """
        Extract all links from description.

        Args:
            description: Description text

        Returns:
            List of link dictionaries
        """
        # URL pattern
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, description)

        links = []
        for i, url in enumerate(urls):
            # Find position in description
            position = description.find(url)
            line_number = description[:position].count('\n') + 1

            # Check if it has anchor text (text before the URL on same line)
            line_start = description.rfind('\n', 0, position)
            if line_start == -1:
                line_start = 0
            line_text = description[line_start:position].strip()

            # Detect link type
            is_affiliate = self._is_affiliate_link(url)
            platform = self._detect_platform(url)

            links.append({
                'url': url,
                'position': position,
                'line_number': line_number,
                'anchor_text': line_text if line_text else None,
                'is_affiliate': is_affiliate,
                'platform': platform
            })

        return links

    def calculate_link_metrics(self, description: str, links: List[Dict]) -> Dict:
        """
        Calculate link-related metrics.

        Args:
            description: Description text
            links: List of extracted links

        Returns:
            Dictionary with link metrics
        """
        word_count = len(description.split())
        total_links = len(links)

        # Link density (links per 100 words)
        link_density = (total_links / word_count * 100) if word_count > 0 else 0

        # Positioning score (1-10)
        # Higher score if links are placed early (first 200 chars is prime real estate)
        positioning_score = 5.0  # Default

        if total_links > 0:
            # Check if any links in first 200 characters
            early_links = sum(1 for link in links if link['position'] < 200)

            if early_links > 0:
                positioning_score = min(10.0, 7.0 + (early_links * 1.5))
            else:
                # Links are further down
                avg_position = sum(link['position'] for link in links) / total_links
                if avg_position < 500:
                    positioning_score = 6.0
                elif avg_position < 1000:
                    positioning_score = 5.0
                else:
                    positioning_score = 4.0

        return {
            'link_density': round(link_density, 2),
            'positioning_score': round(positioning_score, 1),
            'links_in_first_200_chars': sum(1 for link in links if link['position'] < 200),
            'avg_link_position': sum(link['position'] for link in links) / total_links if total_links > 0 else 0
        }

    def analyze_structure(self, description: str) -> Dict:
        """
        Analyze description structure.

        Args:
            description: Description text

        Returns:
            Structure analysis
        """
        lines = description.split('\n')
        non_empty_lines = [line for line in lines if line.strip()]

        # Check for common structural elements
        has_intro = len(non_empty_lines) > 0 and len(non_empty_lines[0]) > 20
        has_sections = any(':' in line for line in non_empty_lines[:5])

        # Estimate readability (simple metric)
        avg_line_length = sum(len(line) for line in non_empty_lines) / len(non_empty_lines) if non_empty_lines else 0

        return {
            'has_intro_paragraph': has_intro,
            'has_sections': has_sections,
            'avg_line_length': round(avg_line_length, 1),
            'blank_lines': len(lines) - len(non_empty_lines),
            'formatting_score': self._calculate_formatting_score(description)
        }

    def analyze_cta(self, description: str) -> Dict:
        """
        Analyze call-to-action presence and effectiveness.

        Args:
            description: Description text

        Returns:
            CTA analysis
        """
        description_lower = description.lower()

        # CTA keywords
        cta_keywords = [
            'click', 'check out', 'download', 'subscribe', 'sign up',
            'get', 'try', 'watch', 'learn', 'discover', 'join',
            'buy', 'shop', 'visit', 'see', 'start', 'grab'
        ]

        # Find CTAs
        cta_examples = []
        cta_count = 0

        for keyword in cta_keywords:
            pattern = rf'\b{keyword}\b[^\n.!?]*[.!?]?'
            matches = re.findall(pattern, description_lower)
            if matches:
                cta_count += len(matches)
                for match in matches[:2]:  # Keep first 2 examples per keyword
                    cta_examples.append(match.strip())

        has_clear_cta = cta_count > 0

        return {
            'has_clear_cta': has_clear_cta,
            'cta_count': cta_count,
            'cta_examples': cta_examples[:5]  # Top 5 examples
        }

    def _ai_analyze_description(self, description: str, title: str) -> Optional[Dict]:
        """
        AI-powered description analysis using Claude.

        Args:
            description: Description text
            title: Video title

        Returns:
            AI analysis results or None
        """
        if not self.client:
            return None

        prompt = f"""
Analyze this YouTube video description for effectiveness (Tech/Software niche):

**TITLE:** {title}

**DESCRIPTION:**
{description}

Provide analysis in JSON format:

{{
  "cta_effectiveness_score": <float 1-10>,
  "description_quality_score": <float 1-10>,
  "seo_score": <float 1-10>,
  "optimization_suggestions": [<array of 3-5 specific suggestions>],
  "missing_elements": [<array of missing important elements>],
  "strengths": [<array of what's done well>]
}}

**Scoring criteria:**

**cta_effectiveness_score** (1-10):
- Are CTAs clear and compelling?
- Are they action-oriented?
- Are they placed strategically?

**description_quality_score** (1-10):
- Is it informative and engaging?
- Does it provide value beyond the video?
- Is it well-structured?

**seo_score** (1-10):
- Keywords present?
- Good for search discovery?
- Proper formatting?

Return ONLY valid JSON, no other text.
"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = message.content[0].text
            return json.loads(response_text)

        except Exception as e:
            logger.error(f"Error in AI description analysis: {e}")
            return None

    def _is_affiliate_link(self, url: str) -> bool:
        """Check if URL is likely an affiliate link."""
        affiliate_indicators = [
            'amzn.to', '/ref=', '?tag=', 'affiliate', 'aff=',
            'referral', 'partner', '/go/', 'geni.us'
        ]
        return any(indicator in url.lower() for indicator in affiliate_indicators)

    def _detect_platform(self, url: str) -> str:
        """Detect platform from URL."""
        url_lower = url.lower()
        if 'amazon' in url_lower or 'amzn' in url_lower:
            return 'Amazon'
        elif 'youtube' in url_lower or 'youtu.be' in url_lower:
            return 'YouTube'
        elif 'twitter' in url_lower or 'x.com' in url_lower:
            return 'Twitter/X'
        elif 'instagram' in url_lower:
            return 'Instagram'
        elif 'facebook' in url_lower:
            return 'Facebook'
        elif 'tiktok' in url_lower:
            return 'TikTok'
        elif 'discord' in url_lower:
            return 'Discord'
        elif 'github' in url_lower:
            return 'GitHub'
        else:
            return 'Other'

    def _has_timestamps(self, description: str) -> bool:
        """Check if description has timestamps."""
        timestamp_pattern = r'\d{1,2}:\d{2}'
        return bool(re.search(timestamp_pattern, description))

    def _has_social_links(self, description: str) -> bool:
        """Check if description has social media links."""
        social_keywords = ['twitter', 'instagram', 'facebook', 'tiktok', 'discord', 'linkedin']
        return any(keyword in description.lower() for keyword in social_keywords)

    def _has_hashtags(self, description: str) -> bool:
        """Check if description has hashtags."""
        return '#' in description

    def _calculate_formatting_score(self, description: str) -> float:
        """Calculate formatting quality score (1-10)."""
        score = 5.0  # Base score

        # Positive factors
        if '\n\n' in description:  # Has paragraph breaks
            score += 1.0
        if self._has_timestamps(description):  # Has timestamps
            score += 1.0
        if len(description.split('\n')) > 5:  # Multi-line
            score += 0.5
        if any(char in description for char in ['•', '-', '*']):  # Has bullet points
            score += 1.0

        # Negative factors
        if len(description) < 100:  # Too short
            score -= 2.0
        if len(description.split('\n')) == 1:  # No line breaks
            score -= 1.0

        return max(1.0, min(10.0, score))
