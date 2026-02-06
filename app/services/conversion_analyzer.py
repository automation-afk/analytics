"""Conversion analysis using Claude API."""
import logging
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
