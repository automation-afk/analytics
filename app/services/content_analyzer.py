"""
Content Analyzer using Claude API
Analyzes video script quality, persuasion techniques, and user intent matching.
"""

from anthropic import Anthropic
import json
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ContentAnalyzer:
    """
    AI-powered video content analysis using Claude API.

    Analyzes:
    - Script quality (1-10)
    - Hook effectiveness (1-10)
    - Call-to-action strength (1-10)
    - Persuasion techniques used
    - User intent matching (1-10)
    - Content structure (intro, CTA, problem-solution)
    - Key strengths and improvement areas
    """

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """
        Initialize content analyzer.

        Args:
            api_key: Anthropic API key
            model: Claude model to use (default: Sonnet 4 for speed/cost)
        """
        self.client = Anthropic(api_key=api_key)
        self.model = model
        logger.info(f"Content analyzer initialized with model: {model}")

    def analyze_script_quality(
        self,
        transcript: str,
        title: str,
        description: str
    ) -> Optional[Dict]:
        """
        Comprehensive script quality analysis.

        Args:
            transcript: Full video transcript
            title: Video title
            description: Video description

        Returns:
            Dictionary with analysis scores and insights
        """
        prompt = self._build_script_analysis_prompt(transcript, title, description)

        try:
            logger.info("Analyzing script quality...")
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                temperature=0.3,  # Lower temperature for more consistent scoring
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

            analysis = json.loads(response_text)

            logger.info(f"Script analysis complete. Quality score: {analysis.get('script_quality_score', 'N/A')}")
            return analysis

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}")
            logger.error(f"Response text: {response_text}")
            return None
        except Exception as e:
            logger.error(f"Error analyzing script: {e}")
            return None

    def _build_script_analysis_prompt(self, transcript: str, title: str, description: str) -> str:
        """Build prompt for script analysis."""
        # Truncate transcript if too long (to stay within token limits)
        max_transcript_length = 15000
        if len(transcript) > max_transcript_length:
            transcript = transcript[:max_transcript_length] + "... [truncated]"

        return f"""
Analyze this YouTube video content for quality and effectiveness in the Tech/Software niche:

**TITLE:** {title}

**DESCRIPTION:** {description}

**TRANSCRIPT:** {transcript}

Provide a detailed analysis in JSON format with the following structure:

{{
  "script_quality_score": <float 1-10>,
  "hook_effectiveness_score": <float 1-10>,
  "call_to_action_score": <float 1-10>,
  "persuasion_effectiveness_score": <float 1-10>,
  "user_intent_match_score": <float 1-10>,
  "persuasion_techniques": [<array of technique names used>],
  "identified_intent": "<what user problem/question this addresses>",
  "intent_satisfaction_score": <float 1-10>,
  "has_clear_intro": <boolean>,
  "has_clear_cta": <boolean>,
  "problem_solution_structure": <boolean>,
  "readability_score": <float 1-10>,
  "key_strengths": [<array of 3-5 key strengths>],
  "improvement_areas": [<array of 3-5 specific improvement suggestions>],
  "target_audience": "<who this content is for>",
  "content_value_score": <float 1-10>
}}

**SCORING GUIDELINES:**

**script_quality_score** (1-10): Overall writing quality
- 1-3: Poor (rambling, unclear, unprofessional)
- 4-6: Average (decent but could be much better)
- 7-8: Good (clear, engaging, professional)
- 9-10: Excellent (exceptional clarity, engagement, value)

**hook_effectiveness_score** (1-10): First 30 seconds
- Does it grab attention immediately?
- Does it clearly state what the video is about?
- Does it create curiosity or urgency?

**call_to_action_score** (1-10): CTA clarity and strength
- Is there a clear CTA?
- Is it specific and actionable?
- Is it placed effectively?

**persuasion_effectiveness_score** (1-10): How persuasive is the content?
- Uses social proof (testimonials, numbers, authority)?
- Creates urgency or scarcity?
- Addresses objections?
- Uses emotional appeals?

**user_intent_match_score** (1-10): Does content match likely search intent?
- 9-10: Perfectly matches what user is looking for
- 7-8: Mostly relevant with some tangents
- 4-6: Partially relevant
- 1-3: Doesn't match intent

**persuasion_techniques**: List ALL techniques detected:
- "social_proof" (testimonials, numbers, case studies)
- "scarcity" (limited time, limited spots)
- "authority" (credentials, expertise, awards)
- "reciprocity" (giving value first)
- "consistency" (getting small commitments)
- "liking" (building rapport, relatability)
- "fear_of_missing_out" (FOMO)
- "urgency" (act now messaging)
- "storytelling" (narratives, examples)
- "contrast" (before/after, comparisons)
- "specificity" (exact numbers, details)
- "pain_point_agitation" (highlighting problems)

Return ONLY valid JSON, no other text or markdown formatting.
"""

    def analyze_batch(
        self,
        videos: list[Dict]
    ) -> list[Dict]:
        """
        Analyze multiple videos in batch.

        Args:
            videos: List of video dictionaries with transcript, title, description

        Returns:
            List of analysis results
        """
        results = []

        for i, video in enumerate(videos, 1):
            logger.info(f"Analyzing video {i}/{len(videos)}: {video.get('title', 'Unknown')}")

            analysis = self.analyze_script_quality(
                transcript=video.get('transcript', ''),
                title=video.get('title', ''),
                description=video.get('description', '')
            )

            if analysis:
                analysis['video_id'] = video.get('video_id')
                results.append(analysis)
            else:
                logger.warning(f"Failed to analyze video {video.get('video_id')}")

        return results

    def quick_score(
        self,
        transcript: str,
        title: str
    ) -> Optional[float]:
        """
        Quick quality score (1-10) without full analysis.
        Faster and cheaper than full analysis.

        Args:
            transcript: Video transcript
            title: Video title

        Returns:
            Quality score (1-10) or None if error
        """
        # Truncate transcript
        max_length = 5000
        if len(transcript) > max_length:
            transcript = transcript[:max_length]

        prompt = f"""
Rate this YouTube video content quality on a scale of 1-10 (Tech/Software niche):

Title: {title}

Transcript: {transcript}

Consider:
- Clarity and structure
- Value provided to viewer
- Engagement and hook
- Professional presentation

Return ONLY a single number (1-10) with one decimal place, nothing else.
Example: 7.5
"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=10,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            score_text = message.content[0].text.strip()
            score = float(score_text)

            if 1 <= score <= 10:
                return score
            else:
                logger.warning(f"Score out of range: {score}")
                return None

        except Exception as e:
            logger.error(f"Error getting quick score: {e}")
            return None
