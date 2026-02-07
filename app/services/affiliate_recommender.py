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
        top_n: int = 5,
        affiliate_performance: list = None
    ) -> Optional[List[Dict]]:
        """
        Recommend affiliate products based on video content and real performance data.

        Args:
            transcript: Full video transcript
            title: Video title
            description: Video description
            top_n: Number of products to recommend (default: 5)
            affiliate_performance: Real BigQuery performance data (list of AffiliatePerformance)

        Returns:
            List of product recommendations with scores and reasoning
        """
        prompt = self._build_recommendation_prompt(
            transcript, title, description, top_n, affiliate_performance
        )

        try:
            logger.info(f"Generating {top_n} affiliate product recommendations...")
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                temperature=0.5,  # Slightly higher temperature for creative recommendations
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON response
            response_text = message.content[0].text

            # Extract JSON from response - handle text/markdown before/after JSON
            response_text = response_text.strip()

            # Try to extract JSON from ```json ... ``` code blocks first
            import re
            json_block = re.search(r'```(?:json)?\s*\n(.*?)```', response_text, re.DOTALL)
            if json_block:
                response_text = json_block.group(1).strip()
            else:
                # Fallback: find the first { and last } to extract JSON object
                first_brace = response_text.find('{')
                last_brace = response_text.rfind('}')
                if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                    response_text = response_text[first_brace:last_brace + 1]

            try:
                data = json.loads(response_text)
            except json.JSONDecodeError:
                # Try to repair truncated JSON by closing open brackets
                repaired = response_text.rstrip().rstrip(',')
                # Count open vs close braces/brackets
                open_braces = repaired.count('{') - repaired.count('}')
                open_brackets = repaired.count('[') - repaired.count(']')
                repaired += '}' * max(open_braces, 0) + ']' * max(open_brackets, 0)
                # Try one more pattern: close any unclosed "products" array
                if '"products"' in repaired and open_brackets > 0:
                    repaired = repaired.rstrip('}').rstrip() .rstrip(',') + ']}'
                data = json.loads(repaired)
                logger.info("Successfully repaired truncated JSON response")

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
            logger.error(f"Response text (first 500 chars): {response_text[:500]}")
            return []
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []

    def _build_recommendation_prompt(
        self,
        transcript: str,
        title: str,
        description: str,
        top_n: int,
        affiliate_performance: list = None
    ) -> str:
        """Build prompt for affiliate recommendations."""
        # Truncate transcript if too long
        max_transcript_length = 10000
        if len(transcript) > max_transcript_length:
            transcript = transcript[:max_transcript_length] + "... [truncated]"

        # Build real performance data section
        perf_section = ""
        existing_products = []
        if affiliate_performance:
            perf_lines = []
            for p in affiliate_performance:
                existing_products.append(p.affiliate)
                perf_lines.append(
                    f"  - {p.affiliate} ({p.platform}) | Placement: {p.link_placement} | "
                    f"Revenue: ${p.total_revenue:.2f} | Clicks: {p.total_clicks} | "
                    f"Sales: {p.total_sales} | Conv: {p.conversion_rate:.1f}% | "
                    f"Rev/Click: ${p.revenue_per_click:.2f}"
                )
            # Deduplicate product names
            unique_products = list(dict.fromkeys(existing_products))
            remaining_slots = max(0, top_n - len(unique_products))

            perf_section = f"""

**REAL AFFILIATE PERFORMANCE DATA (from actual tracking):**
{chr(10).join(perf_lines)}

**MANDATORY RULES WHEN REAL DATA EXISTS:**
1. Your FIRST recommendations MUST be these existing products (they are PROVEN to work): {', '.join(unique_products)}
   - Set mentioned_in_video=true for each
   - Use their REAL conversion rates for conversion_probability
   - Analyze their performance in recommendation_reasoning
2. You have {remaining_slots} remaining slot(s) for NEW product suggestions
3. NEW suggestions must be DIRECT COMPETITORS in the exact same product category
   - Same type of product, same use case, different brand
   - NOT adjacent categories (VPNs are NOT data broker removal, password managers are NOT code editors)
4. If you cannot find {remaining_slots} direct competitors, return fewer products. DO NOT pad with unrelated products.
"""

        return f"""
Analyze this YouTube video and recommend affiliate products.

**TITLE:** {title}

**DESCRIPTION:** {description}

**TRANSCRIPT:** {transcript}
{perf_section}
**STEP 1:** What EXACT product type/category is this video about? Be very specific (e.g., "data broker removal services" NOT "privacy tools", "React frameworks" NOT "web development").

**STEP 2:** {"Include the existing tracked products listed above as your top recommendations, then fill remaining slots with direct competitors ONLY." if affiliate_performance else f"Recommend up to {top_n} products that are the SAME type of product discussed in the video."}

**HARD RULES:**
- Every product must be the SAME product type as the video's topic
- VPNs, antivirus, password managers, courses are DIFFERENT product types from data broker removal
- Hosting providers are DIFFERENT product types from frontend frameworks
- A "related" product in a different category is NOT acceptable
- If the video compares Product A vs Product B vs Product C, then A, B, C should be your recommendations
{"- The existing tracked products (" + ', '.join(existing_products[:5]) + ") MUST appear first in your list" if existing_products else ""}

Return JSON:
{{
  "products": [
    {{
      "product_name": "<specific product name>",
      "product_category": "<the EXACT product type from Step 1>",
      "relevance_score": <float 1-10>,
      "conversion_probability": <float 0-100>,
      "recommendation_reasoning": "<2-3 sentences. Reference real data if available.>",
      "where_to_mention": "<specific part of video or description>",
      "mentioned_in_video": <boolean>,
      "amazon_asin": "<ASIN if applicable, otherwise null>",
      "typical_commission_rate": "<estimated commission %>",
      "price_range": "<low/medium/high>",
      "target_audience_match": "<why this audience needs this exact product>"
    }}
  ]
}}

**relevance_score:** 9-10 = same product type discussed in video, 7-8 = direct competitor, below 7 = do not include.
**conversion_probability:** {"Base on REAL conversion rates from tracked data above." if affiliate_performance else "Be realistic, 20-60% for most."}

Return ONLY valid JSON, no other text.
"""

    @staticmethod
    def analyze_existing_links(
        description: str,
        known_affiliates: list = None
    ) -> Dict:
        """
        Analyze links already in video description.

        Args:
            description: Video description text
            known_affiliates: List of affiliate names from BigQuery (e.g., ["Optery", "DeleteMe", "Incogni"])

        Returns:
            Dictionary with link analysis
        """
        # Extract all URLs from description
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, description)

        # Build set of known affiliate names (lowercased) from BigQuery data
        known_names = set()
        if known_affiliates:
            for name in known_affiliates:
                known_names.add(name.lower())

        # Analyze each URL
        link_analysis = []
        for url in urls:
            platform = AffiliateRecommender._detect_platform(url, known_names)
            is_affiliate = AffiliateRecommender._detect_affiliate_link(url, known_names)
            link_analysis.append({
                'url': url,
                'is_affiliate': is_affiliate,
                'platform': platform
            })

        affiliate_count = sum(1 for link in link_analysis if link['is_affiliate'])
        total_links = len(link_analysis)

        return {
            'total_links': total_links,
            'affiliate_links': affiliate_count,
            'non_affiliate_links': total_links - affiliate_count,
            'links': link_analysis,
            'has_affiliate_disclosure': AffiliateRecommender._check_disclosure(description)
        }

    @staticmethod
    def _detect_affiliate_link(url: str, known_affiliates: set = None) -> bool:
        """
        Detect if URL is likely an affiliate link.

        Args:
            url: URL to analyze
            known_affiliates: Set of lowercase affiliate names from BigQuery

        Returns:
            True if likely affiliate link
        """
        url_lower = url.lower()

        # If a known affiliate name appears in the URL, it's definitely affiliate
        if known_affiliates:
            for name in known_affiliates:
                if name in url_lower:
                    return True

        # Common affiliate network / tracking patterns
        affiliate_indicators = [
            'amzn.to',
            '/ref=',
            '?tag=',
            'affiliate',
            'aff=',
            'referral',
            '/go/',
            'geni.us',
            'click.linksynergy.com',
            'shareasale.com',
            'cj.com',
            'avantlink.com',
            'pxf.io',
            'dpbolvw.net',
            'impact.com',
            'sjv.io',
            'tkqlhce.com',
            'jdoqocy.com',
            'kqzyfj.com',
        ]

        if any(indicator in url_lower for indicator in affiliate_indicators):
            return True

        # URL shorteners in YT descriptions are almost always affiliate/tracking links
        shortener_domains = [
            'bit.ly/', 'tinyurl.com/', 'ow.ly/', 'buff.ly/',
            'rebrand.ly/', 'shorturl.at/', 'cutt.ly/',
        ]
        if any(domain in url_lower for domain in shortener_domains):
            return True

        # Deal/offer subdomains (e.g., deal.incogni.io, try.aura.com)
        if re.search(r'https?://(deal|offer|try|get|go|promo)\.',  url_lower):
            return True

        return False

    @staticmethod
    def _detect_platform(url: str, known_affiliates: set = None) -> str:
        """
        Detect platform/product from URL using BigQuery affiliate names.

        Args:
            url: URL to analyze
            known_affiliates: Set of lowercase affiliate names from BigQuery
        """
        url_lower = url.lower()

        # First check against known affiliate names from BigQuery
        if known_affiliates:
            for name in known_affiliates:
                if name in url_lower:
                    # Return properly capitalized version
                    return name.title()

        # Common affiliate networks
        if 'amazon' in url_lower or 'amzn' in url_lower:
            return 'Amazon'
        elif 'youtu.be' in url_lower or 'youtube.com' in url_lower:
            return 'YouTube'
        elif 'shareasale' in url_lower:
            return 'ShareASale'
        elif 'cj.com' in url_lower or 'linksynergy' in url_lower:
            return 'CJ Affiliate'

        # Try to extract product name from shortener path (e.g., bit.ly/Optery_hWeD1)
        shorteners = ['bit.ly/', 'tinyurl.com/', 'ow.ly/', 'rebrand.ly/', 'cutt.ly/']
        for shortener in shorteners:
            if shortener in url_lower:
                path = url.split(shortener, 1)[-1].split('?')[0]
                match = re.match(r'([A-Za-z]+)', path)
                if match and len(match.group(1)) > 2:
                    extracted = match.group(1)
                    # Check if it matches a known affiliate
                    if known_affiliates and extracted.lower() in known_affiliates:
                        return extracted.title()
                    return extracted
                break

        # Try to extract from deal/offer subdomains (e.g., deal.incogni.io)
        deal_match = re.search(r'https?://(?:deal|offer|try|get|go|promo)\.([^./]+)', url_lower)
        if deal_match:
            return deal_match.group(1).title()

        return 'Other'

    @staticmethod
    def _check_disclosure(description: str) -> bool:
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
        existing = AffiliateRecommender.analyze_existing_links(description)

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
