"""Frame analysis service using vision AI models."""
import os
import base64
import logging
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class FrameAnalyzer:
    """Analyze video frames using vision AI (Claude or GPT-4V)."""

    def __init__(self, anthropic_api_key: str = None, openai_api_key: str = None):
        self.anthropic_api_key = anthropic_api_key or os.getenv('ANTHROPIC_API_KEY')
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')

    def analyze_frames(
        self,
        frame_paths: List[Path],
        provider: str = "claude",
        batch_size: int = 5
    ) -> List[Dict]:
        """
        Analyze frames and return text descriptions.

        Args:
            frame_paths: List of paths to frame images
            provider: 'claude' or 'openai'
            batch_size: Number of frames to analyze in one API call

        Returns:
            List of frame analysis results with descriptions
        """
        if not frame_paths:
            return []

        results = []

        # Process frames in batches to reduce API calls
        for i in range(0, len(frame_paths), batch_size):
            batch = frame_paths[i:i + batch_size]
            try:
                if provider == "openai":
                    batch_results = self._analyze_batch_openai(batch)
                else:
                    batch_results = self._analyze_batch_claude(batch)
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Frame analysis error: {e}")
                # Add placeholder results for failed frames
                for path in batch:
                    results.append({
                        "frame": path.name,
                        "error": str(e)
                    })

        return results

    def _encode_image(self, image_path: Path) -> str:
        """Encode image to base64."""
        with open(image_path, "rb") as f:
            return base64.standard_b64encode(f.read()).decode("utf-8")

    def _analyze_batch_claude(self, frame_paths: List[Path]) -> List[Dict]:
        """Analyze frames using Claude Vision."""
        try:
            import anthropic
        except ImportError:
            raise ImportError("Please install anthropic: pip install anthropic")

        if not self.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY not configured")

        client = anthropic.Anthropic(api_key=self.anthropic_api_key)

        # Build content with all frames
        content = []
        frame_info = []

        for i, path in enumerate(frame_paths):
            # Extract timestamp from filename (e.g., frame_000000_0.0s.jpg)
            timestamp = self._extract_timestamp(path.name)
            frame_info.append({"path": path, "timestamp": timestamp, "index": i})

            # Add image to content
            image_data = self._encode_image(path)
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": image_data
                }
            })

        # Add analysis prompt
        content.append({
            "type": "text",
            "text": f"""Analyze these {len(frame_paths)} video frames. For each frame, provide:
1. Scene description (what's shown)
2. On-screen text (any titles, captions, CTAs visible)
3. Visual quality (lighting, composition)
4. Key objects or people visible

Format as JSON array with one object per frame in order:
[{{"frame": 1, "timestamp": "0.0s", "scene": "...", "text_on_screen": "...", "quality": "...", "key_elements": [...]}}, ...]"""
        })

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": content}]
        )

        # Parse response
        response_text = response.content[0].text

        try:
            import json
            # Extract JSON from response
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1
            if json_start >= 0 and json_end > json_start:
                parsed = json.loads(response_text[json_start:json_end])
                # Add actual timestamps
                for i, item in enumerate(parsed):
                    if i < len(frame_info):
                        item["timestamp"] = frame_info[i]["timestamp"]
                        item["filename"] = frame_info[i]["path"].name
                return parsed
        except json.JSONDecodeError:
            pass

        # Fallback: return raw text
        return [{"raw_analysis": response_text, "frames": [p.name for p in frame_paths]}]

    def _analyze_batch_openai(self, frame_paths: List[Path]) -> List[Dict]:
        """Analyze frames using GPT-4 Vision."""
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Please install openai: pip install openai")

        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY not configured")

        client = OpenAI(api_key=self.openai_api_key)

        # Build content with all frames
        content = []
        frame_info = []

        for i, path in enumerate(frame_paths):
            timestamp = self._extract_timestamp(path.name)
            frame_info.append({"path": path, "timestamp": timestamp, "index": i})

            image_data = self._encode_image(path)
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{image_data}",
                    "detail": "low"  # Use low detail to reduce cost
                }
            })

        content.append({
            "type": "text",
            "text": f"""Analyze these {len(frame_paths)} video frames. For each frame, provide:
1. Scene description (what's shown)
2. On-screen text (any titles, captions, CTAs visible)
3. Visual quality (lighting, composition)
4. Key objects or people visible

Format as JSON array with one object per frame in order:
[{{"frame": 1, "timestamp": "0.0s", "scene": "...", "text_on_screen": "...", "quality": "...", "key_elements": [...]}}, ...]"""
        })

        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Use mini for cost efficiency
            max_tokens=2000,
            messages=[{"role": "user", "content": content}]
        )

        response_text = response.choices[0].message.content

        try:
            import json
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1
            if json_start >= 0 and json_end > json_start:
                parsed = json.loads(response_text[json_start:json_end])
                for i, item in enumerate(parsed):
                    if i < len(frame_info):
                        item["timestamp"] = frame_info[i]["timestamp"]
                        item["filename"] = frame_info[i]["path"].name
                return parsed
        except json.JSONDecodeError:
            pass

        return [{"raw_analysis": response_text, "frames": [p.name for p in frame_paths]}]

    def _extract_timestamp(self, filename: str) -> float:
        """Extract timestamp from frame filename like 'frame_000000_0.0s.jpg'."""
        try:
            # Find the timestamp part (e.g., "0.0s")
            parts = filename.replace('.jpg', '').split('_')
            for part in parts:
                if part.endswith('s'):
                    return float(part[:-1])
        except (ValueError, IndexError):
            pass
        return 0.0

    def analyze_thumbnail(self, image_path: Path, provider: str = "claude") -> Dict:
        """
        Analyze a single thumbnail/image for quality and appeal.

        Returns:
            Dict with thumbnail analysis
        """
        results = self.analyze_frames([image_path], provider, batch_size=1)
        if results:
            result = results[0]
            result["is_thumbnail_analysis"] = True
            return result
        return {"error": "Failed to analyze thumbnail"}
