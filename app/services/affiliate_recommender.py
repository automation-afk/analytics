"""
Affiliate Product Recommender using Claude API
Recommends relevant Tech/Software products based on video content.
"""

from anthropic import Anthropic
import json
import re
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class AffiliateRecommender:
    """
    AI-powered affiliate product recommendation system.

    Specialized for Tech/Software niche:
    - Software tools and SaaS products
    - Online courses and educational platforms
    - Hardware and tech gadgets
    - Development tools and services
    - Productivity and business software
    """

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """
        Initialize affiliate recommender.

        Args:
            api_key: Anthropic API key
            model: Claude model to use
        """
        self.client = Anthropic(api_key=api_key)
        self.model = model
        logger.info(f"Affiliate recommender initialized with model: {model}")

    def recommend_products(
        self,
        transcript: str,
        title: str,
        description: str,
        top_n: int = 5
    ) -> Optional[List[Dict]]:
        """
        Recommend affiliate products based on video content.

        Args:
            transcript: Full video transcript
            title: Video title
            description: Video description
            top_n: Number of products to recommend (default: 5)

        Returns:
            List of product recommendations with scores and reasoning
        """
        prompt = self._build_recommendation_prompt(transcript, title, description, top_n)

        try:
            logger.info(f"Generating {top_n} affiliate product recommendations...")
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                temperature=0.5,  # Slightly higher temperature for creative recommendations
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON response
            response_text = message.content[0].text

            # Strip markdown code blocks if present
            response_text = response_text.strip()
            if response_text.startswith('```'):
                # Remove opening ```json or ```
                response_text = response_text.split('\n', 1)[1] if '\n' in response_text else response_text[3:]
                # Remove closing ```
                if response_text.endswith('```'):
                    response_text = response_text.rsplit('```', 1)[0]
                response_text = response_text.strip()

            data = json.loads(response_text)

            # Extract products list from the JSON response
            if isinstance(data, dict) and 'products' in data:
                products = data['products']
            elif isinstance(data, list):
                products = data
            else:
                logger.error(f"Unexpected response format: {type(data)}")
                return []

            # Normalize scores (convert 1-10 to 0-1 for relevance, percentage to 0-1 for conversion)
            for p in products:
                # Normalize relevance score from 1-10 to 0-1
                if 'relevance_score' in p and p['relevance_score'] > 1:
                    p['relevance_score'] = p['relevance_score'] / 10.0
                # Normalize conversion probability from 0-100 to 0-1
                if 'conversion_probability' in p and p['conversion_probability'] > 1:
                    p['conversion_probability'] = p['conversion_probability'] / 100.0

            logger.info(f"Generated {len(products)} product recommendations")
            return products

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}")
            logger.error(f"Response text: {response_text}")
            return None
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return None

    def _build_recommendation_prompt(
        self,
        transcript: str,
        title: str,
        description: str,
        top_n: int
    ) -> str:
        """Build prompt for affiliate recommendations."""
        # Truncate transcript if too long
        max_transcript_length = 10000
        if len(transcript) > max_transcript_length:
            transcript = transcript[:max_transcript_length] + "... [truncated]"

        return f"""
Based on this YouTube video content in the Tech/Software niche, recommend the top {top_n} most relevant affiliate products:

**TITLE:** {title}

**DESCRIPTION:** {description}

**TRANSCRIPT:** {transcript}

Recommend products from these categories:
1. **SaaS/Software Tools** (productivity, development, design, marketing, analytics)
2. **Online Courses & Education** (coding bootcamps, skill platforms, tutorials)
3. **Tech Hardware** (laptops, monitors, accessories, gadgets)
4. **Developer Tools & Services** (hosting, APIs, frameworks, libraries)
5. **Business Software** (CRM, project management, communication)

For each product, provide:

{{
  "products": [
    {{
      "product_name": "<specific product name>",
      "product_category": "<category from list above>",
      "relevance_score": <float 1-10>,
      "conversion_probability": <float 0-100>,
      "recommendation_reasoning": "<2-3 sentences explaining why this product fits>",
      "where_to_mention": "<specific part of video or description where it should be mentioned>",
      "mentioned_in_video": <boolean - is this already mentioned?>,
      "amazon_asin": "<ASIN if applicable, otherwise null>",
      "typical_commission_rate": "<estimated commission % if known>",
      "price_range": "<low/medium/high or specific price if known>",
      "target_audience_match": "<how well it matches the video's audience>"
    }}
  ]
}}

**SCORING GUIDELINES:**

**relevance_score** (1-10): How relevant is this product to the video content?
- 9-10: Directly solves a problem discussed in the video
- 7-8: Highly relevant to the topic
- 5-6: Somewhat relevant
- 1-4: Loosely related

**conversion_probability** (0-100): Realistic estimate of conversion likelihood
- 80-100%: Viewer will immediately see the value and is ready to buy
- 60-79%: Very likely to convert with good presentation
- 40-59%: Moderate conversion chance
- 20-39%: Lower conversion, but still valuable
- 0-19%: Low conversion probability

Be realistic with conversion probabilities - most products will be in 20-60% range.

**Important:**
- Recommend SPECIFIC products, not generic categories
- Consider what the viewer is trying to accomplish
- Match product complexity to audience level
- Prioritize products with established affiliate programs
- Be honest about fit - a good recommendation is better than forcing irrelevant products

Return ONLY valid JSON in the exact format shown above, no other text.
"""

    def analyze_existing_links(
        self,
        description: str
    ) -> Dict:
        """
        Analyze links already in video description.

        Args:
            description: Video description text

        Returns:
            Dictionary with link analysis
        """
        # Extract all URLs from description
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, description)

        # Analyze each URL
        link_analysis = []
        for url in urls:
            is_affiliate = self._detect_affiliate_link(url)
            link_analysis.append({
                'url': url,
                'is_affiliate': is_affiliate,
                'platform': self._detect_platform(url)
            })

        affiliate_count = sum(1 for link in link_analysis if link['is_affiliate'])
        total_links = len(link_analysis)

        return {
            'total_links': total_links,
            'affiliate_links': affiliate_count,
            'non_affiliate_links': total_links - affiliate_count,
            'links': link_analysis,
            'has_affiliate_disclosure': self._check_disclosure(description)
        }

    def _detect_affiliate_link(self, url: str) -> bool:
        """
        Detect if URL is likely an affiliate link.

        Args:
            url: URL to analyze

        Returns:
            True if likely affiliate link
        """
        # Common affiliate link patterns
        affiliate_indicators = [
            'amzn.to',
            '/ref=',
            '?tag=',
            'affiliate',
            'aff=',
            'referral',
            'partner',
            '/go/',
            'geni.us',
            'click.linksynergy.com',
            'shareasale.com',
            'cj.com',
            'avantlink.com',
            'pxf.io',
            'dpbolvw.net'
        ]

        url_lower = url.lower()
        return any(indicator in url_lower for indicator in affiliate_indicators)

    def _detect_platform(self, url: str) -> str:
        """Detect platform from URL."""
        if 'amazon' in url or 'amzn' in url:
            return 'Amazon'
        elif 'clickbank' in url:
            return 'ClickBank'
        elif 'shareasale' in url:
            return 'ShareASale'
        elif 'cj.com' in url or 'linksynergy' in url:
            return 'CJ Affiliate'
        elif 'gumroad' in url:
            return 'Gumroad'
        elif 'teachable' in url or 'thinkific' in url or 'udemy' in url:
            return 'Course Platform'
        else:
            return 'Other'

    def _check_disclosure(self, description: str) -> bool:
        """Check if description contains affiliate disclosure."""
        disclosure_keywords = [
            'affiliate',
            'commission',
            'compensated',
            'sponsored',
            'partner link',
            'disclosure'
        ]

        description_lower = description.lower()
        return any(keyword in description_lower for keyword in disclosure_keywords)

    def compare_recommendations_to_existing(
        self,
        recommendations: List[Dict],
        description: str
    ) -> Dict:
        """
        Compare AI recommendations to existing links in description.

        Args:
            recommendations: List of recommended products
            description: Video description

        Returns:
            Comparison analysis
        """
        existing = self.analyze_existing_links(description)

        # Check if recommended products are already mentioned
        description_lower = description.lower()
        for rec in recommendations:
            product_name_lower = rec['product_name'].lower()
            rec['already_mentioned'] = product_name_lower in description_lower

        new_opportunities = [r for r in recommendations if not r.get('already_mentioned', False)]

        return {
            'existing_affiliate_links': existing['affiliate_links'],
            'recommended_products': len(recommendations),
            'already_implemented': len(recommendations) - len(new_opportunities),
            'new_opportunities': len(new_opportunities),
            'new_opportunity_products': new_opportunities,
            'has_disclosure': existing['has_affiliate_disclosure']
        }
