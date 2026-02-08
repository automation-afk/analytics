"""Authentication blueprint - Google OAuth-based access control."""
import os
from flask import Blueprint, render_template, redirect, url_for, request, session, flash, current_app, make_response
from functools import wraps
from authlib.integrations.flask_client import OAuth

bp = Blueprint('auth', __name__)


@bp.after_app_request
def add_no_cache_headers(response):
    """Add no-cache headers to prevent browser from caching authenticated pages."""
    if 'user_email' in session:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, private'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# Initialize OAuth
oauth = OAuth()

# Allowed email domains
ALLOWED_DOMAINS = ['digidom.ventures', 'banyantree.digital']


def init_oauth(app):
    """Initialize OAuth with app configuration."""
    oauth.init_app(app)

    # Register Google OAuth provider
    oauth.register(
        name='google',
        client_id=os.getenv('GOOGLE_OAUTH_CLIENT_ID'),
        client_secret=os.getenv('GOOGLE_OAUTH_CLIENT_SECRET'),
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )


def login_required(f):
    """Decorator to require login for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_email' not in session:
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


def is_email_allowed(email):
    """Check if email domain is in allowed list."""
    if not email or '@' not in email:
        return False

    domain = email.split('@')[1].lower()
    return domain in ALLOWED_DOMAINS


@bp.route('/login', methods=['GET'])
def login():
    """Login page with Google OAuth."""
    # If already logged in, redirect to dashboard
    if 'user_email' in session:
        return redirect(url_for('dashboard.overview'))

    return render_template('auth/login.html')


@bp.route('/login/google')
def login_google():
    """Initiate Google OAuth flow."""
    # Get the callback URL
    redirect_uri = url_for('auth.authorize', _external=True)

    # Redirect to Google for authentication
    return oauth.google.authorize_redirect(redirect_uri)


@bp.route('/authorize')
def authorize():
    """Handle Google OAuth callback."""
    # If already logged in, skip OAuth flow (prevents double-callback issues)
    if session.get('user_email'):
        return redirect(url_for('dashboard.overview'))

    try:
        # Get the token from Google
        token = oauth.google.authorize_access_token()

        # Get user info from Google (userinfo is included in token with openid scope)
        user_info = token.get('userinfo')
        if not user_info:
            # Fallback: fetch userinfo from Google's userinfo endpoint
            user_info = oauth.google.userinfo()

        # Extract email
        email = user_info.get('email', '').lower()

        # Check if email domain is allowed
        if not is_email_allowed(email):
            flash(f'Access denied. Only @digidom.ventures and @banyantree.digital emails are allowed.', 'error')
            return redirect(url_for('auth.login'))

        # Check if email is verified by Google
        if not user_info.get('email_verified', False):
            flash('Please verify your email with Google first.', 'error')
            return redirect(url_for('auth.login'))

        # Store user info in session
        session['user_email'] = email
        session['user_name'] = user_info.get('name', '')
        session['user_picture'] = user_info.get('picture', '')
        session.permanent = True

        # Log login to Google Sheets (don't let logging break login)
        try:
            if current_app.activity_logger:
                current_app.activity_logger.log_login(email)
        except Exception:
            current_app.logger.warning(f'Failed to log login for {email}')

        first_name = user_info.get('given_name') or user_info.get('name', 'User').split()[0]
        flash(f'Welcome, {first_name}!', 'success')

        # Redirect to next page or dashboard
        next_page = request.args.get('next')
        if next_page:
            return redirect(next_page)
        return redirect(url_for('dashboard.overview'))

    except Exception as e:
        current_app.logger.error(f'OAuth error: {str(e)}')
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('auth.login'))


@bp.route('/logout')
def logout():
    """Logout and clear session completely."""
    # Log logout before clearing session
    email = session.get('user_email')
    if email and current_app.activity_logger:
        current_app.activity_logger.log_logout(email)

    # Clear all session data
    session.clear()

    # Create response with redirect
    response = make_response(redirect(url_for('auth.login')))

    # Add no-cache headers to prevent back button showing cached logged-in pages
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, private'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'

    # Clear session cookie
    response.delete_cookie('session')

    flash('You have been logged out successfully.', 'success')
    return response
