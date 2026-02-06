"""Transcription service for YouTube videos using Whisper APIs."""
import os
import logging
import subprocess
import requests
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import tempfile

logger = logging.getLogger(__name__)


class TranscriptionService:
    """
    Service for transcribing YouTube videos with optional:
    - Frame extraction + AI analysis (Analyze & Discard approach)
    - Voice emotion analysis (Hume AI)

    Uses RapidAPI for YouTube downloads (works on Railway/production)
    Falls back to yt-dlp for local development
    """

    def __init__(
        self,
        groq_api_key: str = None,
        openai_api_key: str = None,
        anthropic_api_key: str = None,
        hume_api_key: str = None,
        rapidapi_key: str = None
    ):
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        self.anthropic_api_key = anthropic_api_key or os.getenv('ANTHROPIC_API_KEY')
        self.hume_api_key = hume_api_key or os.getenv('HUME_API_KEY')
        self.rapidapi_key = rapidapi_key or os.getenv('RAPIDAPI_KEY')

        # Initialize multimodal analyzer if Anthropic key is available
        self.multimodal_analyzer = None
        if self.anthropic_api_key:
            from app.services.multimodal_analyzer import MultimodalAnalyzer
            self.multimodal_analyzer = MultimodalAnalyzer(self.anthropic_api_key)
            logger.info("MultimodalAnalyzer initialized for content insights")

        if not self.groq_api_key and not self.openai_api_key:
            logger.warning("No transcription API keys configured")

        # Check download method availability
        if self.rapidapi_key:
            logger.info("RapidAPI key configured - will use for YouTube downloads")
        else:
            logger.warning("No RapidAPI key - will try yt-dlp (may not work on production)")

    def transcribe_video(
        self,
        video_id: str,
        frame_interval: int = None,
        provider: str = "groq",
        store_segments: bool = False,
        analyze_frames: bool = False,
        analyze_emotions: bool = False,
        generate_transcript: bool = True,
        generate_insights: bool = True,
        existing_data: dict = None,
        progress_callback: callable = None
    ) -> Optional[Dict]:
        """
        Transcribe a YouTube video with optional analysis.

        Args:
            video_id: YouTube video ID
            frame_interval: Seconds between frame extractions (None = no frames)
            provider: 'groq' (free) or 'openai' (paid)
            store_segments: Whether to include timestamp segments
            analyze_frames: Analyze frames with vision AI (stores text, discards images)
            analyze_emotions: Analyze voice emotions with Hume AI
            generate_transcript: Generate new transcript (if False, use existing)
            generate_insights: Generate AI content insights
            existing_data: Existing transcript data from database (for merging)
            progress_callback: Optional callback(step, progress, message) for progress updates

        Returns:
            Dict with transcript and analysis data or None if failed
        """
        # Store progress callback for use in download methods
        self._progress_callback = progress_callback

        def update_progress(step: str, progress: int, message: str = ""):
            """Update progress via callback if provided."""
            if progress_callback:
                try:
                    progress_callback(step, progress, message)
                except Exception as e:
                    logger.warning(f"Progress callback error: {e}")

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        should_extract_frames = analyze_frames and frame_interval is not None
        needs_download = generate_transcript or analyze_emotions or analyze_frames

        # Create temp directory for downloads (auto-cleanup)
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            try:
                video_path = None
                video_info = None
                audio_info = None
                frames = []
                frame_analysis = None
                emotions = None
                audio_path = None
                transcript_result = None
                info = {}

                # Use existing data if available
                if existing_data:
                    info = {
                        'title': existing_data.get('title'),
                        'channel': existing_data.get('channel'),
                        'description': existing_data.get('description'),
                        'duration': existing_data.get('duration_seconds')
                    }

                if needs_download:
                    update_progress('download', 2, 'Initializing download...')

                    # Try RapidAPI first (works on production), then yt-dlp (local fallback)
                    download_urls = None
                    if self.rapidapi_key:
                        update_progress('download', 3, 'Fetching download URLs from RapidAPI...')
                        download_urls = self._get_rapidapi_urls(video_id)
                        if download_urls:
                            info = download_urls.get('info', info)
                            logger.info(f"Got RapidAPI URLs for {video_id}")

                    # Download video if extracting frames
                    if should_extract_frames:
                        logger.info(f"Downloading video for frame extraction: {video_id}")
                        update_progress('download', 5, 'Downloading video for frame extraction...')

                        if download_urls and download_urls.get('video_url'):
                            video_path = self._download_from_url(
                                download_urls['video_url'],
                                temp_path / f"{video_id}.mp4",
                                'video'
                            )
                            video_info = info
                        else:
                            # Fallback to yt-dlp
                            video_path, video_info = self._download_video_ytdlp(video_url, temp_path)

                        if video_path and video_path.exists():
                            logger.info(f"Extracting frames every {frame_interval}s")
                            update_progress('frames', 22, f'Extracting frames every {frame_interval}s...')
                            frames, frame_paths = self._extract_frames(video_path, temp_path, frame_interval)
                            update_progress('frames', 28, f'Extracted {len(frames)} frames')

                            # Analyze frames with vision AI (Analyze & Discard)
                            if frame_paths:
                                logger.info(f"Analyzing {len(frame_paths)} frames with vision AI...")
                                update_progress('frames', 30, f'Analyzing {len(frame_paths)} frames with AI vision...')
                                frame_analysis = self._analyze_frames(frame_paths)
                                update_progress('frames', 45, 'Frame analysis complete')

                            # Extract audio from video (avoid re-downloading)
                            if generate_transcript or analyze_emotions:
                                update_progress('download', 46, 'Extracting audio from video...')
                                audio_path = self._extract_audio_from_video(video_path, temp_path)

                    # Download audio if needed and not already extracted
                    if (generate_transcript or analyze_emotions) and (not audio_path or not audio_path.exists()):
                        logger.info(f"Downloading audio: {video_id}")
                        update_progress('download', 5, 'Downloading audio...')

                        if download_urls and download_urls.get('audio_url'):
                            audio_path = self._download_from_url(
                                download_urls['audio_url'],
                                temp_path / f"{video_id}_audio.m4a",
                                'audio'
                            )
                            audio_info = info
                        else:
                            # Fallback to yt-dlp
                            audio_path, audio_info = self._download_audio_ytdlp(video_url, temp_path)

                    info = video_info or audio_info or info

                    # Transcribe if enabled
                    if generate_transcript:
                        if not audio_path or not audio_path.exists():
                            logger.error("Failed to get audio for transcription")
                            return None

                        update_progress('download', 20, 'Audio ready for transcription')
                        logger.info(f"Transcribing with {provider}...")
                        update_progress('transcribe', 50, f'Transcribing audio with {provider}...')
                        transcript_result = self._transcribe_with_fallback(audio_path, provider)

                        if not transcript_result:
                            return None

                        update_progress('transcribe', 65, 'Transcription complete')

                    # Analyze emotions with Hume AI if enabled
                    if analyze_emotions:
                        if audio_path and audio_path.exists():
                            logger.info("Analyzing voice emotions with Hume AI...")
                            update_progress('emotions', 70, 'Analyzing voice emotions with Hume AI...')
                            emotions = self._analyze_emotions(audio_path)
                            update_progress('emotions', 82, 'Emotion analysis complete')
                        else:
                            logger.warning("No audio available for emotion analysis")

                # Determine final data (new or existing)
                final_transcript = transcript_result['text'] if transcript_result else (existing_data.get('transcript') if existing_data else None)
                final_emotions = emotions if analyze_emotions else (existing_data.get('emotions') if existing_data else None)
                final_frames = frame_analysis if analyze_frames else (existing_data.get('frame_analysis') if existing_data else None)

                # Run multimodal AI analysis if enabled
                content_insights = None
                if generate_insights and self.multimodal_analyzer:
                    logger.info("Running multimodal content analysis...")
                    update_progress('insights', 85, 'Generating AI content insights...')
                    content_insights = self.multimodal_analyzer.analyze_content(
                        transcript=final_transcript or '',
                        emotions=final_emotions,
                        frame_analysis=final_frames,
                        title=info.get('title', ''),
                        duration_seconds=info.get('duration', 0)
                    )
                    update_progress('insights', 92, 'Content insights complete')

                # Build result
                update_progress('save', 94, 'Building results...')
                result = {
                    'video_id': video_id,
                    'title': info.get('title'),
                    'channel': info.get('channel') or info.get('uploader'),
                    'description': info.get('description'),
                    'duration_seconds': info.get('duration'),
                    'transcript': final_transcript,
                    'word_count': len(final_transcript.split()) if final_transcript else 0,
                    'provider': transcript_result.get('provider', provider) if transcript_result else (existing_data.get('provider') if existing_data else 'existing'),
                    'segments': transcript_result.get('segments') if transcript_result and store_segments else None,
                    'frame_count': len(frames) if frames else 0,
                    'frame_interval_seconds': frame_interval,
                    'frame_timestamps': [f['timestamp'] for f in frames] if frames else None,
                    'frame_analysis': final_frames,
                    'emotions': final_emotions,
                    'content_insights': content_insights
                }

                logger.info(f"Processing complete: {result.get('word_count', 0)} words")
                update_progress('save', 95, 'Saving to database...')
                return result

            except Exception as e:
                logger.error(f"Processing error: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return None

    def _get_rapidapi_urls(self, video_id: str) -> Optional[Dict]:
        """
        Get video/audio download URLs from RapidAPI (YouTube Media Downloader).
        Returns both URLs in a single API call (1 request = 1 video).
        """
        if not self.rapidapi_key:
            return None

        url = "https://youtube-media-downloader.p.rapidapi.com/v2/video/details"
        headers = {
            "x-rapidapi-key": self.rapidapi_key,
            "x-rapidapi-host": "youtube-media-downloader.p.rapidapi.com"
        }
        params = {"videoId": video_id}

        try:
            logger.info(f"Fetching RapidAPI URLs for video: {video_id}")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                logger.error(f"RapidAPI error: {response.status_code} - {response.text[:200]}")
                return None

            data = response.json()

            # Get best audio URL (prefer m4a for compatibility with Whisper)
            audio_url = None
            audios = data.get('audios', {}).get('items', [])
            for audio in audios:
                if audio.get('extension') in ['m4a', 'mp3']:
                    audio_url = audio.get('url')
                    break
            if not audio_url and audios:
                audio_url = audios[0].get('url')

            # Get best video URL (720p or lower for reasonable file size)
            video_url = None
            videos = data.get('videos', {}).get('items', [])
            for video in videos:
                quality = video.get('quality', '')
                if '720' in quality or '480' in quality or '360' in quality:
                    video_url = video.get('url')
                    break
            if not video_url and videos:
                video_url = videos[0].get('url')

            return {
                'audio_url': audio_url,
                'video_url': video_url,
                'info': {
                    'title': data.get('title'),
                    'channel': data.get('channel', {}).get('name'),
                    'description': data.get('description'),
                    'duration': data.get('lengthSeconds')
                }
            }

        except Exception as e:
            logger.error(f"RapidAPI request error: {str(e)}")
            return None

    def _download_from_url(self, url: str, output_path: Path, media_type: str = 'audio') -> Optional[Path]:
        """Download file from direct URL (from RapidAPI)."""
        try:
            logger.info(f"Downloading {media_type} from URL...")
            response = requests.get(url, timeout=300, stream=True)

            if response.status_code != 200:
                logger.error(f"Download failed: {response.status_code}")
                return None

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0 and self._progress_callback:
                            pct = downloaded / total_size
                            progress = int(5 + (pct * 13))  # 5-18% range
                            self._progress_callback('download', progress, f'Downloading {media_type}... {pct*100:.0f}%')

            logger.info(f"Downloaded {media_type} to: {output_path} ({output_path.stat().st_size / 1024 / 1024:.2f} MB)")
            return output_path

        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return None

    def _download_video_ytdlp(self, video_url: str, output_path: Path) -> Tuple[Optional[Path], Optional[Dict]]:
        """Download video from YouTube using yt-dlp (fallback for local development)."""
        try:
            import yt_dlp
        except ImportError:
            logger.error("yt-dlp not installed and RapidAPI not available")
            return None, None

        def progress_hook(d):
            """Track download progress and report via callback."""
            if d['status'] == 'downloading':
                if self._progress_callback:
                    downloaded = d.get('downloaded_bytes', 0)
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    if total > 0:
                        pct = downloaded / total
                        progress = int(5 + (pct * 13))
                        speed = d.get('speed', 0)
                        speed_str = f"{speed/1024/1024:.1f} MB/s" if speed else ""
                        self._progress_callback('download', progress, f'Downloading video... {pct*100:.0f}% {speed_str}')
            elif d['status'] == 'finished':
                if self._progress_callback:
                    self._progress_callback('download', 18, 'Download complete, processing...')

        ydl_opts = {
            'format': 'bestvideo[height<=720]+bestaudio/bestvideo+bestaudio/best',
            'outtmpl': str(output_path / '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'merge_output_format': 'mp4',
            'progress_hooks': [progress_hook],
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                video_id = info['id']
                video_file = output_path / f"{video_id}.mp4"

                if not video_file.exists():
                    for f in output_path.glob(f"{video_id}.*"):
                        if f.suffix in ['.mp4', '.webm', '.mkv']:
                            video_file = f
                            break

                return video_file, info
        except Exception as e:
            logger.error(f"yt-dlp video download error: {str(e)}")
            return None, None

    def _download_audio_ytdlp(self, video_url: str, output_path: Path) -> Tuple[Optional[Path], Optional[Dict]]:
        """Download audio from YouTube using yt-dlp (fallback for local development)."""
        try:
            import yt_dlp
        except ImportError:
            logger.error("yt-dlp not installed and RapidAPI not available")
            return None, None

        def progress_hook(d):
            """Track download progress and report via callback."""
            if d['status'] == 'downloading':
                if self._progress_callback:
                    downloaded = d.get('downloaded_bytes', 0)
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    if total > 0:
                        pct = downloaded / total
                        progress = int(5 + (pct * 13))
                        speed = d.get('speed', 0)
                        speed_str = f"{speed/1024/1024:.1f} MB/s" if speed else ""
                        self._progress_callback('download', progress, f'Downloading audio... {pct*100:.0f}% {speed_str}')
            elif d['status'] == 'finished':
                if self._progress_callback:
                    self._progress_callback('download', 18, 'Download complete, converting to mp3...')

        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': str(output_path / '%(id)s_audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '64',
            }],
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [progress_hook],
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                video_id = info['id']
                audio_file = output_path / f"{video_id}_audio.mp3"
                return audio_file, info
        except Exception as e:
            logger.error(f"yt-dlp audio download error: {str(e)}")
            return None, None

    def _extract_audio_from_video(self, video_path: Path, output_path: Path) -> Optional[Path]:
        """Extract audio from video file using ffmpeg."""
        audio_file = output_path / f"{video_path.stem}_audio.mp3"

        cmd = [
            'ffmpeg', '-i', str(video_path),
            '-vn',
            '-acodec', 'libmp3lame',
            '-ab', '64k',
            '-y',
            str(audio_file)
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            logger.info(f"Audio extracted from video: {audio_file}")
            return audio_file
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else e}")
            return None
        except FileNotFoundError:
            logger.error("FFmpeg not found - cannot extract audio from video")
            return None

    def _extract_frames(self, video_path: Path, output_dir: Path, interval_seconds: int) -> tuple:
        """Extract frames from video at specified interval."""
        try:
            import cv2
        except ImportError:
            logger.warning("OpenCV not installed, skipping frame extraction")
            return [], []

        frames_dir = output_dir / f"{video_path.stem}_frames"
        frames_dir.mkdir(exist_ok=True)

        cap = cv2.VideoCapture(str(video_path))
        fps = cap.get(cv2.CAP_PROP_FPS)

        if fps <= 0:
            logger.warning("Could not determine video FPS")
            return [], []

        frames = []
        frame_paths = []
        frame_interval = int(fps * interval_seconds)
        frame_count = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_count % frame_interval == 0:
                timestamp = frame_count / fps
                frame_filename = f"frame_{frame_count:06d}_{timestamp:.1f}s.jpg"
                frame_path = frames_dir / frame_filename

                cv2.imwrite(str(frame_path), frame)
                frames.append({
                    "frame_number": frame_count,
                    "timestamp": round(timestamp, 2),
                    "filename": frame_filename,
                })
                frame_paths.append(frame_path)

            frame_count += 1

        cap.release()
        logger.info(f"Extracted {len(frames)} frames")
        return frames, frame_paths

    def _analyze_frames(self, frame_paths: List[Path]) -> Optional[List[Dict]]:
        """Analyze frames using vision AI."""
        try:
            from app.services.frame_analyzer import FrameAnalyzer

            analyzer = FrameAnalyzer(
                anthropic_api_key=self.anthropic_api_key,
                openai_api_key=self.openai_api_key
            )

            # Always use OpenAI for frame analysis (cost efficient: ~$0.005/video vs ~$0.36)
            provider = "openai"
            return analyzer.analyze_frames(frame_paths, provider=provider, batch_size=5)
        except Exception as e:
            logger.error(f"Frame analysis error: {e}")
            return None

    def _analyze_emotions(self, audio_path: Path) -> Optional[Dict]:
        """Analyze voice emotions using Hume AI."""
        try:
            from app.services.emotion_analyzer import EmotionAnalyzer

            analyzer = EmotionAnalyzer(api_key=self.hume_api_key)
            return analyzer.analyze_audio(audio_path)
        except Exception as e:
            logger.error(f"Emotion analysis error: {e}")
            return None

    def _transcribe_with_fallback(self, audio_path: Path, preferred_provider: str = "groq") -> Optional[Dict]:
        """Transcribe audio with automatic fallback from Groq to OpenAI."""

        if preferred_provider == "openai":
            return self._transcribe_with_openai(audio_path)

        try:
            return self._transcribe_with_groq(audio_path)
        except Exception as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ['rate_limit', '429', 'quota', '413', 'too large']):
                logger.warning("Groq limit reached, falling back to OpenAI")
                if self.openai_api_key:
                    return self._transcribe_with_openai(audio_path)
            raise

    def _transcribe_with_groq(self, audio_path: Path) -> Dict:
        """Transcribe using Groq Whisper API."""
        from groq import Groq

        if not self.groq_api_key:
            raise ValueError("Groq API key not configured")

        client = Groq(api_key=self.groq_api_key)

        with open(audio_path, "rb") as audio_file:
            transcription = client.audio.transcriptions.create(
                file=(audio_path.name, audio_file.read()),
                model="whisper-large-v3",
                response_format="verbose_json",
                language="en",
            )

        segments = None
        if hasattr(transcription, 'segments') and transcription.segments:
            segments = [
                {
                    "start": seg.get("start") if isinstance(seg, dict) else getattr(seg, "start", None),
                    "end": seg.get("end") if isinstance(seg, dict) else getattr(seg, "end", None),
                    "text": seg.get("text") if isinstance(seg, dict) else getattr(seg, "text", None),
                }
                for seg in transcription.segments
            ]

        return {"text": transcription.text, "segments": segments, "provider": "groq"}

    def _transcribe_with_openai(self, audio_path: Path) -> Dict:
        """Transcribe using OpenAI Whisper API."""
        from openai import OpenAI

        if not self.openai_api_key:
            raise ValueError("OpenAI API key not configured")

        client = OpenAI(api_key=self.openai_api_key)

        with open(audio_path, "rb") as audio_file:
            transcription = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="verbose_json",
                language="en",
            )

        segments = None
        if hasattr(transcription, 'segments') and transcription.segments:
            segments = [
                {"start": seg.start, "end": seg.end, "text": seg.text}
                for seg in transcription.segments
            ]

        return {"text": transcription.text, "segments": segments, "provider": "openai"}
