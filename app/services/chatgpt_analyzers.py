"""ChatGPT-based analyzers using OpenAI API."""
import logging
from typing import Dict, List, Optional
from openai import OpenAI
import json

logger = logging.getLogger(__name__)


class ChatGPTContentAnalyzer:
    """Content analyzer using ChatGPT."""

    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview"):
        """Initialize ChatGPT content analyzer."""
        self.client = OpenAI(api_key=api_key)
        self.model = model
        logger.info(f"ChatGPT content analyzer initialized with model: {model}")

    def analyze_script_quality(self, transcript: str, title: str, description: str) -> Optional[Dict]:
        """Analyze script quality using ChatGPT."""
        try:
            prompt = f"""Analyze this YouTube video script and provide detailed quality metrics.

Video Title: {title}
Description: {description[:200]}...
Transcript: {transcript[:3000]}...

Provide analysis in JSON format with these exact fields:
{{
    "script_quality_score": <float 1-10>,
    "hook_effectiveness_score": <float 1-10>,
    "call_to_action_score": <float 1-10>,
    "persuasion_effectiveness_score": <float 1-10>,
    "user_intent_match_score": <float 1-10>,
    "content_value_score": <float 1-10>,
    "readability_score": <float 1-10>,
    "persuasion_techniques": [<list of techniques used>],
    "key_strengths": [<list of 3-5 strengths>],
    "improvement_areas": [<list of 3-5 areas to improve>],
    "target_audience": "<description>",
    "identified_intent": "<intent>",
    "has_clear_intro": <bool>,
    "has_clear_cta": <bool>,
    "problem_solution_structure": <bool>
}}

Be objective and data-driven."""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert video content analyzer. Respond only with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            logger.info("ChatGPT script analysis completed")
            return result

        except Exception as e:
            logger.error(f"ChatGPT script analysis error: {str(e)}")
            return None


class ChatGPTDescriptionAnalyzer:
    """Description analyzer using ChatGPT."""

    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview"):
        """Initialize ChatGPT description analyzer."""
        self.client = OpenAI(api_key=api_key)
        self.model = model
        logger.info(f"ChatGPT description analyzer initialized")

    def analyze(self, description: str, title: str = "") -> Dict:
        """Analyze video description using ChatGPT."""
        try:
            prompt = f"""Analyze this YouTube video description for CTR and conversion optimization.

Video Title: {title}
Description:
{description}

Provide analysis in JSON format with these exact fields:
{{
    "cta_effectiveness_score": <float 1-10>,
    "description_quality_score": <float 1-10>,
    "seo_score": <float 1-10>,
    "total_links": <int>,
    "affiliate_links": <int>,
    "link_positioning_score": <float 1-10>,
    "has_clear_cta": <bool>,
    "optimization_suggestions": [<list of suggestions>],
    "missing_elements": [<list of missing elements>],
    "strengths": [<list of strengths>]
}}"""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at YouTube description optimization. Respond only with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            logger.info("ChatGPT description analysis completed")
            return result

        except Exception as e:
            logger.error(f"ChatGPT description analysis error: {str(e)}")
            return {}


class ChatGPTAffiliateRecommender:
    """Affiliate product recommender using ChatGPT."""

    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview"):
        """Initialize ChatGPT affiliate recommender."""
        self.client = OpenAI(api_key=api_key)
        self.model = model
        logger.info(f"ChatGPT affiliate recommender initialized")

    def recommend_products(self, transcript: str, title: str, description: str, top_n: int = 5) -> Optional[List[Dict]]:
        """Recommend affiliate products using ChatGPT."""
        try:
            prompt = f"""Based on this video content, recommend {top_n} affiliate products that would be relevant.

Video Title: {title}
Description: {description[:200]}...
Transcript: {transcript[:3000]}...

Provide recommendations in JSON format as an array of products:
[
    {{
        "product_name": "<name>",
        "product_category": "<category>",
        "relevance_score": <float 1-10>,
        "conversion_probability": <float 1-10>,
        "recommendation_reasoning": "<why this product>",
        "where_to_mention": "<timestamp or section>",
        "mentioned_in_video": <bool>,
        "price_range": "<range>"
    }}
]

Focus on products that genuinely match the video content."""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at affiliate marketing and product recommendations. Respond only with valid JSON array."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.4,
                response_format={"type": "json_object"}
            )

            content = response.choices[0].message.content
            result = json.loads(content)

            # Handle both array and object with products key
            if isinstance(result, list):
                products = result
            elif isinstance(result, dict) and 'products' in result:
                products = result['products']
            else:
                products = []

            logger.info(f"ChatGPT recommended {len(products)} products")
            return products[:top_n]

        except Exception as e:
            logger.error(f"ChatGPT affiliate recommendation error: {str(e)}")
            return []
