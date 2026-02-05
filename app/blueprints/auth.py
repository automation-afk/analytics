"""Authentication blueprint - email-based access control."""
from flask import Blueprint, render_template, redirect, url_for, request, session, flash
from functools import wraps

bp = Blueprint('auth', __name__)

# Allowed email domains
ALLOWED_DOMAINS = ['digidom.ventures', 'banyantreedigital.com']


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


@bp.route('/login', methods=['GET', 'POST'])
def login():
    """Login page with email verification."""
    # If already logged in, redirect to dashboard
    if 'user_email' in session:
        return redirect(url_for('dashboard.overview'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()

        if is_email_allowed(email):
            # Store email in session
            session['user_email'] = email
            session.permanent = True  # Keep session alive

            # Redirect to next page or dashboard
            next_page = request.args.get('next')
            if next_page:
                return redirect(next_page)
            return redirect(url_for('dashboard.overview'))
        else:
            flash('Access denied. Only @digidom.ventures and @banyantreedigital.com emails are allowed.', 'error')

    return render_template('auth/login.html')


@bp.route('/logout')
def logout():
    """Logout and clear session."""
    session.clear()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('auth.login'))
