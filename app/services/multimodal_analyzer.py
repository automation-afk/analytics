"""Multimodal Content Analyzer - Combines transcript, emotions, and frames for AI insights."""
import logging
from typing import Dict, List, Optional
from anthropic import Anthropic
import json

logger = logging.getLogger(__name__)


class MultimodalAnalyzer:
    """Analyze video content using transcript, emotion, and frame data together."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """Initialize multimodal analyzer with Claude API."""
        self.client = Anthropic(api_key=api_key)
        self.model = model
        logger.info(f"MultimodalAnalyzer initialized with model: {model}")

    def analyze_content(
        self,
        transcript: str,
        emotions: Optional[Dict] = None,
        frame_analysis: Optional[List[Dict]] = None,
        title: str = "",
        duration_seconds: int = 0
    ) -> Dict:
        """
        Generate comprehensive AI insights by combining transcript, emotions, and frames.

        Args:
            transcript: Full video transcript text
            emotions: Hume AI emotion analysis data
            frame_analysis: List of frame descriptions with timestamps
            title: Video title
            duration_seconds: Video duration

        Returns:
            Dictionary with content insights
        """
        try:
            # Build context sections
            transcript_section = self._build_transcript_section(transcript)
            emotion_section = self._build_emotion_section(emotions)
            frame_section = self._build_frame_section(frame_analysis)

            # Skip if no meaningful data
            if not transcript and not emotions and not frame_analysis:
                logger.warning("No data available for multimodal analysis")
                return self._empty_result("No transcript, emotion, or frame data available")

            duration_str = f"{duration_seconds // 60}m {duration_seconds % 60}s" if duration_seconds else "Unknown"

            prompt = f"""Analyze this YouTube video content using the available multimodal data:

**VIDEO TITLE:** {title or 'Unknown'}
**DURATION:** {duration_str}

{transcript_section}

{emotion_section}

{frame_section}

Based on ALL available data, provide comprehensive insights in JSON format:

{{
    "content_summary": "<2-3 sentence summary of what the video covers>",
    "key_moments": [
        {{"timestamp": "<time or range>", "description": "<what happens>", "significance": "<why it matters>"}}
    ],
    "emotional_arc": "<describe how emotions evolve through the video>",
    "visual_storytelling": "<how visuals support the narrative>",
    "audience_engagement_factors": [
        "<factor 1>",
        "<factor 2>",
        "<factor 3>"
    ],
    "content_strengths": [
        "<strength 1>",
        "<strength 2>",
        "<strength 3>"
    ],
    "improvement_opportunities": [
        "<opportunity 1>",
        "<opportunity 2>"
    ],
    "predicted_audience": "<who would find this content valuable>",
    "content_type": "<educational/entertainment/review/tutorial/opinion/etc>",
    "engagement_score": <1-10 based on content quality and presentation>,
    "key_takeaway": "<one sentence main message of the video>"
}}

**Guidelines:**
- Focus on actionable insights that could improve future content
- Identify moments where emotion and content align (or don't)
- Note visual elements that enhance or detract from the message
- Consider viewer psychology and engagement patterns
- Be specific with timestamps when referencing key moments

Return ONLY valid JSON, no other text.
"""

            # Call Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2500,
                temperature=0.4,
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
            result['analysis_status'] = 'success'
            logger.info(f"Multimodal analysis completed successfully")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in multimodal analysis: {str(e)}")
            return self._empty_result(f"Response parsing failed: {str(e)[:50]}")

        except Exception as e:
            logger.error(f"Error in multimodal analysis: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return self._empty_result(f"Analysis error: {str(e)[:100]}")

    def _build_transcript_section(self, transcript: str) -> str:
        """Build transcript section for prompt."""
        if not transcript:
            return "**TRANSCRIPT:** Not available"

        # Truncate if too long (keep beginning and end for context)
        if len(transcript) > 4000:
            mid_point = len(transcript) // 2
            transcript = transcript[:1800] + "\n\n[... middle section truncated ...]\n\n" + transcript[-1800:]

        return f"""**TRANSCRIPT:**
{transcript}"""

    def _build_emotion_section(self, emotions: Optional[Dict]) -> str:
        """Build emotion analysis section for prompt."""
        if not emotions:
            return "**VOICE EMOTION DATA:** Not available"

        sections = ["**VOICE EMOTION DATA:**"]

        # Handle different emotion data formats
        total_segments = emotions.get('total_segments', 0)
        summary = emotions.get('summary', [])

        # Summary can be a list (new format) or dict (old format)
        if isinstance(summary, list) and summary:
            # New format: summary is a list of top emotions
            sections.append(f"- Segments Analyzed: {total_segments}")
            sections.append(f"- Unique Emotions Detected: {len(summary)}")
            top_emotions = ", ".join([f"{e.get('emotion', 'Unknown')} ({e.get('average_score', 0)*100:.1f}%)" for e in summary[:5]])
            sections.append(f"- Top Emotions: {top_emotions}")
        elif isinstance(summary, dict):
            # Old format: summary is a dict with nested structure
            sections.append(f"- Segments Analyzed: {summary.get('total_segments', total_segments)}")
            sections.append(f"- Unique Emotions Detected: {summary.get('unique_emotions', 0)}")
            if summary.get('top_emotions'):
                top_emotions = ", ".join([f"{e['emotion']} ({e.get('percentage', e.get('average_score', 0)*100):.1f}%)" for e in summary['top_emotions'][:5]])
                sections.append(f"- Top Emotions: {top_emotions}")

        # Add timeline samples - check both 'segments' (new) and 'timeline' (old) keys
        timeline = emotions.get('segments', emotions.get('timeline', []))
        if timeline:
            sections.append("\nEmotion Timeline (samples):")

            # Show first 8 entries
            for entry in timeline[:8]:
                # Handle both 'start' and 'start_time' keys
                start_time = entry.get('start', entry.get('start_time', 0))
                time_str = f"{start_time:.1f}s"
                top_emo = entry.get('top_emotions', [])[:3]
                emo_str = ", ".join([f"{e.get('emotion', 'Unknown')} {e.get('score', 0)*100:.0f}%" for e in top_emo])
                sections.append(f"  {time_str}: {emo_str}")

            if len(timeline) > 13:
                sections.append("  ...")

            # Show last 5 entries for ending emotions
            for entry in timeline[-5:]:
                start_time = entry.get('start', entry.get('start_time', 0))
                time_str = f"{start_time:.1f}s"
                top_emo = entry.get('top_emotions', [])[:3]
                emo_str = ", ".join([f"{e.get('emotion', 'Unknown')} {e.get('score', 0)*100:.0f}%" for e in top_emo])
                sections.append(f"  {time_str}: {emo_str}")

        return "\n".join(sections)

    def _build_frame_section(self, frame_analysis: Optional[List[Dict]]) -> str:
        """Build frame analysis section for prompt."""
        if not frame_analysis:
            return "**VISUAL FRAME ANALYSIS:** Not available"

        sections = ["**VISUAL FRAME ANALYSIS:**"]
        sections.append(f"Total Frames Analyzed: {len(frame_analysis)}")
        sections.append("")

        # Sample frames evenly throughout video
        if len(frame_analysis) > 12:
            # Take beginning, middle samples, and end
            indices = [0, 1, 2]  # First 3
            step = len(frame_analysis) // 6
            indices.extend([step * 2, step * 3, step * 4])  # Middle samples
            indices.extend([len(frame_analysis) - 3, len(frame_analysis) - 2, len(frame_analysis) - 1])  # Last 3
            sampled_frames = [frame_analysis[i] for i in indices if i < len(frame_analysis)]
        else:
            sampled_frames = frame_analysis

        for frame in sampled_frames:
            timestamp = frame.get('timestamp', 0)
            description = frame.get('description', 'No description')
            text_detected = frame.get('text_detected', '')

            time_str = f"{timestamp}s" if isinstance(timestamp, (int, float)) else timestamp
            sections.append(f"[{time_str}] {description[:200]}")
            if text_detected:
                sections.append(f"    Text visible: {text_detected[:100]}")

        return "\n".join(sections)

    def _empty_result(self, reason: str) -> Dict:
        """Return empty result structure with error reason."""
        return {
            'content_summary': reason,
            'key_moments': [],
            'emotional_arc': 'Not analyzed',
            'visual_storytelling': 'Not analyzed',
            'audience_engagement_factors': [],
            'content_strengths': [],
            'improvement_opportunities': [],
            'predicted_audience': 'Unknown',
            'content_type': 'Unknown',
            'engagement_score': 0,
            'key_takeaway': reason,
            'analysis_status': 'failed'
        }
