"""Voice emotion analysis service using Hume AI."""
import os
import json
import time
import logging
import requests
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class EmotionAnalyzer:
    """Analyze voice emotions using Hume AI Expression Measurement API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('HUME_API_KEY')
        self.base_url = "https://api.hume.ai/v0/batch"

    def analyze_audio(self, audio_path: Path, top_n: int = 5) -> Optional[Dict]:
        """
        Analyze voice emotions from audio file.

        Args:
            audio_path: Path to audio file (mp3, wav, etc.)
            top_n: Number of top emotions to return per segment

        Returns:
            Dict with emotion analysis results or None if failed
        """
        if not self.api_key:
            logger.warning("HUME_API_KEY not configured, skipping emotion analysis")
            return None

        if not audio_path.exists():
            logger.error(f"Audio file not found: {audio_path}")
            return None

        logger.info(f"Analyzing emotions with Hume AI: {audio_path.name}")

        try:
            # Step 1: Submit job
            job_id = self._submit_job(audio_path)
            if not job_id:
                return None

            logger.info(f"Hume job started: {job_id}")

            # Step 2: Poll for completion
            if not self._wait_for_completion(job_id):
                return None

            # Step 3: Get and parse results
            return self._get_results(job_id, top_n)

        except Exception as e:
            logger.error(f"Emotion analysis error: {e}")
            return None

    def _submit_job(self, audio_path: Path) -> Optional[str]:
        """Submit audio file to Hume AI batch API."""
        headers = {'X-Hume-Api-Key': self.api_key}

        job_config = json.dumps({
            "models": {
                "prosody": {"granularity": "utterance"}
            }
        })

        with open(audio_path, 'rb') as audio_file:
            files = {
                'file': (audio_path.name, audio_file, 'audio/mpeg'),
                'json': (None, job_config, 'application/json')
            }
            response = requests.post(
                f'{self.base_url}/jobs',
                files=files,
                headers=headers
            )

        if response.status_code != 200:
            logger.error(f"Hume API error: {response.status_code} - {response.text}")
            return None

        return response.json().get('job_id')

    def _wait_for_completion(self, job_id: str, max_wait: int = 300) -> bool:
        """Poll for job completion."""
        headers = {'X-Hume-Api-Key': self.api_key}
        poll_interval = 5
        elapsed = 0

        while elapsed < max_wait:
            response = requests.get(
                f'{self.base_url}/jobs/{job_id}',
                headers=headers
            )
            status_data = response.json()
            status = status_data.get('state', {}).get('status', 'UNKNOWN')

            if status == "COMPLETED":
                logger.info(f"Hume analysis completed in {elapsed}s")
                return True
            elif status == "FAILED":
                logger.error(f"Hume job failed: {status_data}")
                return False

            logger.debug(f"Waiting for Hume... ({elapsed}s, status: {status})")
            time.sleep(poll_interval)
            elapsed += poll_interval

        logger.error("Hume AI job timed out after 5 minutes")
        return False

    def _get_results(self, job_id: str, top_n: int) -> Dict:
        """Get and parse emotion predictions."""
        headers = {'X-Hume-Api-Key': self.api_key}

        response = requests.get(
            f'{self.base_url}/jobs/{job_id}/predictions',
            headers=headers
        )

        if response.status_code != 200:
            logger.error(f"Failed to get predictions: {response.text}")
            return None

        predictions_data = response.json()
        return self._parse_predictions(predictions_data, top_n)

    def _parse_predictions(self, predictions_data: list, top_n: int) -> Dict:
        """Parse Hume AI predictions into structured format."""
        emotion_segments = []
        overall_emotions = {}

        for prediction in predictions_data:
            results = prediction.get('results', {})
            predictions_list = results.get('predictions', [])

            for pred in predictions_list:
                models = pred.get('models', {})
                prosody = models.get('prosody', {})
                grouped = prosody.get('grouped_predictions', [])

                for group in grouped:
                    segments = group.get('predictions', [])

                    for segment in segments:
                        time_info = segment.get('time', {})
                        time_begin = time_info.get('begin', 0)
                        time_end = time_info.get('end', 0)

                        emotions_list = segment.get('emotions', [])
                        sorted_emotions = sorted(
                            emotions_list,
                            key=lambda x: x.get('score', 0),
                            reverse=True
                        )

                        top_emotions = []
                        for emo in sorted_emotions[:top_n]:
                            name = emo.get('name', 'Unknown')
                            score = emo.get('score', 0)
                            top_emotions.append({
                                "emotion": name,
                                "score": round(score, 3)
                            })
                            if name not in overall_emotions:
                                overall_emotions[name] = []
                            overall_emotions[name].append(score)

                        emotion_segments.append({
                            "start": round(time_begin, 2),
                            "end": round(time_end, 2),
                            "top_emotions": top_emotions
                        })

        # Calculate overall emotion averages
        emotion_summary = []
        for name, scores in overall_emotions.items():
            avg_score = sum(scores) / len(scores)
            emotion_summary.append({
                "emotion": name,
                "average_score": round(avg_score, 3),
                "occurrences": len(scores)
            })

        emotion_summary.sort(key=lambda x: x["average_score"], reverse=True)

        return {
            "provider": "hume",
            "segments": emotion_segments,
            "summary": emotion_summary[:10],
            "total_segments": len(emotion_segments)
        }


def get_emotion_insights(emotions_data: Dict) -> Dict:
    """
    Generate insights from emotion analysis.

    Args:
        emotions_data: Output from EmotionAnalyzer.analyze_audio()

    Returns:
        Dict with actionable insights
    """
    if not emotions_data or not emotions_data.get('summary'):
        return {"insights": []}

    summary = emotions_data['summary']
    insights = []

    # Find dominant emotions
    top_3 = summary[:3]
    dominant = top_3[0] if top_3 else None

    if dominant:
        emotion = dominant['emotion']
        score = dominant['average_score']

        # Categorize emotions
        positive_emotions = ['Joy', 'Interest', 'Excitement', 'Admiration', 'Amusement']
        persuasive_emotions = ['Determination', 'Concentration', 'Interest', 'Realization']
        trust_emotions = ['Calmness', 'Contentment', 'Satisfaction', 'Relief']
        urgent_emotions = ['Surprise', 'Excitement', 'Anxiety', 'Fear']

        if emotion in positive_emotions:
            insights.append(f"Strong positive energy ({emotion}: {score:.0%}) - engages viewers")
        if emotion in persuasive_emotions:
            insights.append(f"Persuasive tone detected ({emotion}: {score:.0%}) - good for CTAs")
        if emotion in trust_emotions:
            insights.append(f"Trust-building tone ({emotion}: {score:.0%}) - builds credibility")
        if emotion in urgent_emotions:
            insights.append(f"Urgency detected ({emotion}: {score:.0%}) - drives action")

    # Check for monotony
    if len(summary) >= 5:
        score_variance = max(e['average_score'] for e in summary[:5]) - min(e['average_score'] for e in summary[:5])
        if score_variance < 0.1:
            insights.append("Low emotional variety - consider more dynamic delivery")

    return {
        "dominant_emotion": dominant['emotion'] if dominant else None,
        "confidence": dominant['average_score'] if dominant else 0,
        "insights": insights
    }
