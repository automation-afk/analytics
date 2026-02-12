"""Conversion analysis using Claude API."""
import logging
import re
from typing import Dict, List, Optional
from anthropic import Anthropic
import json

logger = logging.getLogger(__name__)


class ConversionAnalyzer:
    """Analyze conversion performance using Claude AI."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """Initialize conversion analyzer with Claude API."""
        self.client = Anthropic(api_key=api_key)
        self.model = model
        logger.info(f"ConversionAnalyzer initialized with model: {model}")

    def analyze_conversion_drivers(
        self,
        transcript: str,
        title: str,
        description: str,
        revenue: float,
        clicks: int,
        sales: int,
        views: int,
        script_quality_score: Optional[float] = None,
        cta_score: Optional[float] = None
    ) -> Dict:
        """
        Analyze what drives conversions for this video.

        Args:
            transcript: Video transcript
            title: Video title
            description: Video description
            revenue: Total revenue
            clicks: Total affiliate clicks
            sales: Total sales
            views: Total views
            script_quality_score: Script quality score (1-10)
            cta_score: Call-to-action score (1-10)

        Returns:
            Dictionary with conversion analysis
        """
        try:
            # Calculate metrics
            conversion_rate = (sales / clicks * 100) if clicks > 0 else 0.0
            revenue_per_click = (revenue / clicks) if clicks > 0 else 0.0
            click_through_rate = (clicks / views * 100) if views > 0 else 0.0

            # Build context for Claude
            desc_text = (description[:300] + '...') if description else 'No description available'
            transcript_text = (transcript[:2000] + '...') if transcript else 'No transcript available'

            context = f"""
Video Title: {title or 'Unknown'}
Description: {desc_text}
Transcript: {transcript_text}

Performance Metrics:
- Total Revenue: ${revenue:,.2f}
- Total Clicks: {clicks:,}
- Total Sales: {sales}
- Total Views: {views:,}
- Conversion Rate: {conversion_rate:.2f}% (sales/clicks)
- Revenue per Click: ${revenue_per_click:.2f}
- Click-Through Rate: {click_through_rate:.2f}% (clicks/views)
"""

            if script_quality_score:
                context += f"- Script Quality Score: {script_quality_score}/10\n"
            if cta_score:
                context += f"- CTA Score: {cta_score}/10\n"

            prompt = f"""Analyze this YouTube video's conversion performance and identify what drives (or hinders) affiliate sales.

{context}

Provide analysis in JSON format:
{{
    "conversion_drivers": [
        "<3-5 specific elements that drive conversions>"
    ],
    "underperformance_reasons": [
        "<3-5 reasons why conversion might be low, if applicable>"
    ],
    "recommendations": [
        "<3-5 actionable recommendations to improve conversion>"
    ],
    "performance_assessment": "<overall assessment: excellent/good/average/poor>",
    "key_insight": "<one sentence key insight about this video's conversion performance>"
}}

Focus on:
1. How the script builds trust and urgency
2. Quality and placement of CTAs
3. Product-market fit and relevance
4. Viewer intent alignment
5. Comparison to typical benchmarks (good conversion: >10%, excellent: >20%)
"""

            # Call Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                temperature=0.3,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )

            # Parse response
            result_text = response.content[0].text

            # Extract JSON from response (Claude might wrap it in markdown)
            if "```json" in result_text:
                json_start = result_text.find("```json") + 7
                json_end = result_text.find("```", json_start)
                result_text = result_text[json_start:json_end].strip()
            elif "```" in result_text:
                json_start = result_text.find("```") + 3
                json_end = result_text.find("```", json_start)
                result_text = result_text[json_start:json_end].strip()

            result = json.loads(result_text)
            logger.info(f"Conversion analysis completed successfully")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in conversion analysis: {str(e)}")
            logger.error(f"Raw response: {result_text[:500] if 'result_text' in dir() else 'N/A'}")
            return {
                "conversion_drivers": ["Analysis completed but response parsing failed"],
                "underperformance_reasons": [],
                "recommendations": ["Try re-analyzing the video"],
                "performance_assessment": "unknown",
                "key_insight": "Response parsing error - the AI response was not valid JSON"
            }
        except Exception as e:
            logger.error(f"Error in conversion analysis: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "conversion_drivers": [f"Analysis error: {str(e)[:100]}"],
                "underperformance_reasons": [],
                "recommendations": ["Check the logs for more details", "Try re-analyzing the video"],
                "performance_assessment": "unknown",
                "key_insight": f"Analysis failed - see logs for details"
            }

    def score_cta_and_description(
        self,
        title: str,
        description: str,
        silo: str = '',
        keyword: str = '',
        preferred_brand: str = None,
        desc_brand: str = '',
        comment_brand: str = ''
    ) -> Dict:
        """
        Score a video's CTA and description quality using Claude.

        Args:
            title: Video title
            description: Video description text
            silo: Content silo
            keyword: Target keyword
            preferred_brand: The preferred brand for this silo (e.g. "Aura")
            desc_brand: Current top brand in description links
            comment_brand: Current top brand in pinned comment

        Returns:
            Dict with cta_score, description_score, base_score, reasoning
        """
        try:
            desc_text = description[:1500] if description else 'No description available'

            prompt = f"""Score this YouTube video's CTA (Call-to-Action) effectiveness and description quality for affiliate conversions.

Video Title: {title or 'Unknown'}
Target Keyword: {keyword or 'Unknown'}
Content Silo: {silo or 'Unknown'}
Current Description Brand: {desc_brand or 'None'}
Current Comment Brand: {comment_brand or 'None'}

Description:
{desc_text}

Score the following on a scale of 1-10:
1. **CTA Score**: How effective are the calls-to-action? Consider: urgency, clarity, placement, number of CTAs, link visibility, discount/offer mentions
2. **Description Score**: How well-optimized is the description for affiliate conversions? Consider: first-fold link placement, benefit-driven copy, trust signals, keyword usage, link formatting

Return JSON only:
{{
    "cta_score": <1-10>,
    "description_score": <1-10>,
    "reasoning": "<2-3 sentence explanation of scores>"
}}"""

            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}]
            )

            result_text = response.content[0].text

            # Extract JSON
            if "```json" in result_text:
                json_start = result_text.find("```json") + 7
                json_end = result_text.find("```", json_start)
                result_text = result_text[json_start:json_end].strip()
            elif "```" in result_text:
                json_start = result_text.find("```") + 3
                json_end = result_text.find("```", json_start)
                result_text = result_text[json_start:json_end].strip()

            result = json.loads(result_text)
            cta_score = float(result.get('cta_score', 5))
            desc_score = float(result.get('description_score', 5))
            reasoning = result.get('reasoning', '')

            # Base score = average of CTA and description scores
            base_score = round((cta_score + desc_score) / 2, 1)

            # Check if preferred brand is present
            has_preferred = False
            if preferred_brand:
                brand_lower = preferred_brand.lower()
                desc_lower = (description or '').lower()
                has_preferred = brand_lower in desc_lower

            # Apply 50% penalty if preferred brand is missing
            adjusted_score = base_score
            if preferred_brand and not has_preferred:
                adjusted_score = round(base_score * 0.5, 1)

            return {
                'cta_score': cta_score,
                'description_score': desc_score,
                'base_score': base_score,
                'has_preferred_brand': has_preferred,
                'adjusted_score': adjusted_score,
                'reasoning': reasoning
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in CTA scoring: {str(e)}")
            return self._default_cta_score()
        except Exception as e:
            logger.error(f"Error in CTA scoring: {str(e)}")
            return self._default_cta_score()

    @staticmethod
    def _default_cta_score() -> Dict:
        return {
            'cta_score': 0, 'description_score': 0, 'base_score': 0,
            'has_preferred_brand': False, 'adjusted_score': 0,
            'reasoning': 'Scoring failed'
        }
