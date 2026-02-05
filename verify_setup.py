"""Verification script to check if the Flask app is set up correctly."""
import sys
import os

def check_imports():
    """Check if all required modules can be imported."""
    print("Checking imports...")
    errors = []

    try:
        import flask
        print("[OK] Flask installed")
    except ImportError as e:
        errors.append(f"[ERROR] Flask not installed: {e}")

    try:
        from google.cloud import bigquery
        print("[OK] Google Cloud BigQuery installed")
    except ImportError as e:
        errors.append(f"[ERROR] Google Cloud BigQuery not installed: {e}")

    try:
        import anthropic
        print("[OK] Anthropic SDK installed")
    except ImportError as e:
        errors.append(f"[ERROR] Anthropic SDK not installed: {e}")

    try:
        from flask_caching import Cache
        print("[OK] Flask-Caching installed")
    except ImportError as e:
        errors.append(f"[ERROR] Flask-Caching not installed: {e}")

    return errors


def check_files():
    """Check if all required files exist."""
    print("\nChecking files...")
    errors = []

    required_files = [
        'app.py',
        'config.py',
        '.env.web',
        'requirements.txt',
        'app/__init__.py',
        'app/models.py',
        'app/extensions.py',
        'app/services/bigquery_service.py',
        'app/services/analysis_service.py',
        'app/blueprints/dashboard.py',
        'app/blueprints/videos.py',
        'app/blueprints/analysis.py',
        'app/blueprints/api.py',
        'app/templates/base.html',
        'app/templates/dashboard/overview.html',
        'app/static/css/main.css',
        'app/static/js/main.js'
    ]

    for file in required_files:
        if os.path.exists(file):
            print(f"[OK] {file}")
        else:
            errors.append(f"[ERROR] {file} not found")

    return errors


def check_config():
    """Check if configuration is set up."""
    print("\nChecking configuration...")
    errors = []

    if os.path.exists('.env.web'):
        with open('.env.web', 'r') as f:
            content = f.read()
            if 'GOOGLE_CREDENTIALS_PATH' in content:
                print("[OK] GOOGLE_CREDENTIALS_PATH configured")
            else:
                errors.append("[ERROR] GOOGLE_CREDENTIALS_PATH not found in .env.web")

            if 'ANTHROPIC_API_KEY' in content:
                print("[OK] ANTHROPIC_API_KEY configured")
            else:
                errors.append("[ERROR] ANTHROPIC_API_KEY not found in .env.web")

            if 'BIGQUERY_PROJECT_ID' in content:
                print("[OK] BIGQUERY_PROJECT_ID configured")
            else:
                errors.append("[ERROR] BIGQUERY_PROJECT_ID not found in .env.web")
    else:
        errors.append("[ERROR] .env.web file not found")

    return errors


def check_app_import():
    """Try to import the Flask app."""
    print("\nChecking Flask app import...")
    try:
        from app import create_app
        print("[OK] Flask app can be imported")
        return []
    except Exception as e:
        return [f"[ERROR] Error importing Flask app: {e}"]


def main():
    """Run all checks."""
    print("=" * 60)
    print("YouTube Analytics Dashboard - Setup Verification")
    print("=" * 60)

    all_errors = []

    # Run checks
    all_errors.extend(check_imports())
    all_errors.extend(check_files())
    all_errors.extend(check_config())
    all_errors.extend(check_app_import())

    # Summary
    print("\n" + "=" * 60)
    if all_errors:
        print("SETUP INCOMPLETE - Issues found:")
        print("=" * 60)
        for error in all_errors:
            print(error)
        print("\nPlease fix the issues above before running the application.")
        return 1
    else:
        print("SETUP COMPLETE [OK]")
        print("=" * 60)
        print("\nAll checks passed! You can now run the application:")
        print("\n  1. Activate virtual environment:")
        print("     venv\\Scripts\\activate")
        print("\n  2. Run the Flask app:")
        print("     python app.py")
        print("\n  3. Open browser to:")
        print("     http://localhost:5000")
        return 0


if __name__ == '__main__':
    sys.exit(main())
