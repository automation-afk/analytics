"""Analytics service for tracking user activity."""
import logging
import os
import json
from datetime import datetime
from typing import Optional
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Philippine timezone
PH_TZ = pytz.timezone('Asia/Manila')


class AnalyticsService:
    """Service for tracking user activity."""

    def __init__(self, credentials_path: str = None, spreadsheet_id: str = None):
        """
        Initialize analytics client.

        Args:
            credentials_path: Path to Google Cloud service account JSON file
            spreadsheet_id: Target spreadsheet ID
        """
        self.spreadsheet_id = spreadsheet_id or os.getenv(
            'LOGGING_SPREADSHEET_ID',
            '1i8N4mP_-Bj-l7ZCcCwsqCA7CCS7q1h4dHV0s1gCycKw'
        )
        self.sheet_name = 'logs'
        self.service = None

        try:
            # Try to load credentials from environment variable first (for cloud deployment)
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')

            if credentials_json:
                logger.info("Loading credentials from GOOGLE_CREDENTIALS_JSON")
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets"
                    ],
                )
            elif credentials_path and os.path.exists(credentials_path):
                logger.info(f"Loading credentials from file: {credentials_path}")
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets"
                    ],
                )
            else:
                logger.warning("No credentials available for analytics service")
                return

            self.service = build('sheets', 'v4', credentials=credentials)
            logger.info(f"Analytics service initialized")

            # Ensure header row exists
            self._ensure_header()

        except Exception as e:
            logger.error(f"Failed to initialize analytics service: {str(e)}")
            self.service = None

    def _ensure_header(self):
        """Ensure the header row exists in the sheet."""
        if not self.service:
            return

        try:
            # Check if header exists
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{self.sheet_name}!A1:E1'
            ).execute()

            values = result.get('values', [])

            if not values or values[0] != ['Date', 'Time (PH)', 'Email', 'Action', 'Details']:
                # Add header row
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f'{self.sheet_name}!A1:E1',
                    valueInputOption='RAW',
                    body={
                        'values': [['Date', 'Time (PH)', 'Email', 'Action', 'Details']]
                    }
                ).execute()
                logger.info("Created header row")

        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"Sheet '{self.sheet_name}' not found. Please create it manually.")
            else:
                logger.error(f"Error checking header: {str(e)}")
        except Exception as e:
            logger.error(f"Error ensuring header: {str(e)}")

    def log_action(self, email: str, action: str, details: str = None):
        """
        Log a user action.

        Args:
            email: User's email address
            action: Action performed (e.g., "Login", "View Video", "Run Analysis")
            details: Additional details about the action
        """
        if not self.service:
            logger.debug(f"Analytics disabled. Would log: {email} - {action}")
            return

        try:
            # Get current time in Philippine timezone
            now = datetime.now(PH_TZ)
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Prepare row data
            row = [date_str, time_str, email or 'Unknown', action, details or '']

            # Append to sheet
            self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f'{self.sheet_name}!A:E',
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={
                    'values': [row]
                }
            ).execute()

            logger.debug(f"Logged action: {email} - {action}")

        except HttpError as e:
            logger.error(f"Failed to log action: {str(e)}")
        except Exception as e:
            logger.error(f"Error logging action: {str(e)}")

    # Convenience methods for common actions
    def log_login(self, email: str):
        """Log user login."""
        self.log_action(email, 'Login', 'User logged in')

    def log_logout(self, email: str):
        """Log user logout."""
        self.log_action(email, 'Logout', 'User logged out')

    def log_view_dashboard(self, email: str):
        """Log dashboard view."""
        self.log_action(email, 'View Dashboard', 'Viewed main dashboard')

    def log_view_videos_list(self, email: str, filters: dict = None):
        """Log videos list view."""
        details = None
        if filters:
            filter_parts = []
            if filters.get('channel'):
                filter_parts.append(f"channel={filters['channel']}")
            if filters.get('video_id'):
                filter_parts.append(f"video_id={filters['video_id']}")
            if filters.get('has_analysis') is not None:
                filter_parts.append(f"has_analysis={filters['has_analysis']}")
            if filters.get('page'):
                filter_parts.append(f"page={filters['page']}")
            if filter_parts:
                details = ', '.join(filter_parts)
        self.log_action(email, 'View Videos List', details)

    def log_view_video_detail(self, email: str, video_id: str, video_title: str = None):
        """Log video detail view."""
        details = f"video_id={video_id}"
        if video_title:
            details += f", title={video_title[:50]}"
        self.log_action(email, 'View Video Detail', details)

    def log_start_analysis(self, email: str, video_id: str, analysis_types: list = None):
        """Log analysis start."""
        details = f"video_id={video_id}"
        if analysis_types:
            details += f", types={','.join(analysis_types)}"
        self.log_action(email, 'Start Analysis', details)

    def log_start_transcription(self, email: str, video_id: str, options: dict = None):
        """Log transcription start."""
        details = f"video_id={video_id}"
        if options:
            opts = []
            if options.get('transcript'):
                opts.append('transcript')
            if options.get('emotions'):
                opts.append('emotions')
            if options.get('frames'):
                opts.append('frames')
            if options.get('insights'):
                opts.append('insights')
            if opts:
                details += f", options={','.join(opts)}"
        self.log_action(email, 'Start Transcription', details)

    def log_view_analysis_page(self, email: str):
        """Log analysis trigger page view."""
        self.log_action(email, 'View Analysis Page', 'Viewed analysis trigger page')

    def log_view_history(self, email: str):
        """Log history page view."""
        self.log_action(email, 'View History', 'Viewed transcription history')

    def log_batch_analysis(self, email: str, count: int, channel: str = None):
        """Log batch analysis start."""
        details = f"count={count}"
        if channel:
            details += f", channel={channel}"
        self.log_action(email, 'Start Batch Analysis', details)

    def log_download_transcript(self, email: str, video_id: str):
        """Log transcript download."""
        self.log_action(email, 'Download Transcript', f"video_id={video_id}")

    def log_error(self, email: str, error_type: str, error_message: str = None):
        """Log an error."""
        details = error_message[:200] if error_message else None
        self.log_action(email, f'Error: {error_type}', details)
