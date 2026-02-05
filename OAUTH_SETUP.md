# Google OAuth Setup Guide

## Overview
This guide will help you set up Google OAuth authentication for your YouTube Analytics Dashboard. This ensures only verified Google accounts with @digidom.ventures and @banyantreedigital.com domains can access the app.

---

## Step 1: Create Google OAuth Credentials

### 1.1 Go to Google Cloud Console
Visit: https://console.cloud.google.com/

### 1.2 Select Your Project
Select your existing project: `company-wide-370010`

### 1.3 Enable Google+ API
1. Go to **APIs & Services** → **Library**
2. Search for **"Google+ API"** or **"Google Identity"**
3. Click **Enable**

### 1.4: Create OAuth Consent Screen
1. Go to **APIs & Services** → **OAuth consent screen**
2. Select **Internal** (for workspace domain users only)
3. Fill in:
   - **App name**: YouTube Analytics Dashboard
   - **User support email**: your-email@digidom.ventures
   - **App logo**: (optional)
   - **Authorized domains**:
     - digidom.ventures
     - banyantreedigital.com
   - **Developer contact**: your-email@digidom.ventures
4. Click **Save and Continue**
5. **Scopes**: Click **Add or Remove Scopes**
   - Select: `.../auth/userinfo.email`
   - Select: `.../auth/userinfo.profile`
   - Select: `openid`
6. Click **Save and Continue**

### 1.5: Create OAuth Client ID
1. Go to **APIs & Services** → **Credentials**
2. Click **+ CREATE CREDENTIALS** → **OAuth client ID**
3. Application type: **Web application**
4. Name: `YT Analytics Dashboard`
5. **Authorized JavaScript origins**:
   - http://localhost:5000
   - http://127.0.0.1:5000
   - http://192.168.1.7:5000 (your local IP)
   - https://your-render-app.onrender.com (for production)

6. **Authorized redirect URIs**:
   - http://localhost:5000/authorize
   - http://127.0.0.1:5000/authorize
   - https://your-render-app.onrender.com/authorize (for production)

7. Click **CREATE**

### 1.6: Copy Your Credentials
You'll see a popup with:
- **Client ID**: `123456789-abcdefg.apps.googleusercontent.com`
- **Client secret**: `GOCSPX-...`

**Copy both of these!**

---

## Step 2: Update .env.web File

Open `.env.web` and update these lines:

```env
GOOGLE_OAUTH_CLIENT_ID=123456789-abcdefg.apps.googleusercontent.com
GOOGLE_OAUTH_CLIENT_SECRET=GOCSPX-your-secret-here
```

---

## Step 3: Install Dependencies

```bash
cd c:\Python\desktop_python_files\running_scripts\yt_analytics\ClaudeCode\web_app
pip install -r requirements.txt
```

---

## Step 4: Test Locally

### 4.1 Start the App
```bash
python app.py
```

### 4.2 Visit Login Page
Go to: http://localhost:5000/login

### 4.3 Click "Sign in with Google"
- You'll be redirected to Google
- Sign in with your @digidom.ventures or @banyantreedigital.com email
- Grant permissions
- You'll be redirected back and logged in!

---

## Step 5: Deploy to Render

### 5.1 Update OAuth Redirect URI
1. Go back to Google Cloud Console → Credentials
2. Edit your OAuth Client ID
3. Add production redirect URI:
   ```
   https://your-app-name.onrender.com/authorize
   ```
4. Add production JavaScript origin:
   ```
   https://your-app-name.onrender.com
   ```

### 5.2 Add Environment Variables in Render
In your Render dashboard, add these environment variables:
```
GOOGLE_OAUTH_CLIENT_ID=123456789-abcdefg.apps.googleusercontent.com
GOOGLE_OAUTH_CLIENT_SECRET=GOCSPX-your-secret-here
GOOGLE_CREDENTIALS_JSON=<your-bigquery-credentials-json>
ANTHROPIC_API_KEY=<your-anthropic-key>
BIGQUERY_PROJECT_ID=company-wide-370010
CACHE_TYPE=simple
LOG_LEVEL=INFO
```

### 5.3 Push to GitHub
```bash
git add .
git commit -m "Add Google OAuth authentication"
git push
```

Render will automatically deploy!

---

## Troubleshooting

### Error: "redirect_uri_mismatch"
**Solution**: Make sure your redirect URI in Google Cloud Console exactly matches what your app is using:
- Local: `http://localhost:5000/authorize`
- Production: `https://your-app.onrender.com/authorize`

### Error: "Access denied"
**Solution**: Check that your email domain (@digidom.ventures or @banyantreedigital.com) is in the ALLOWED_DOMAINS list in `auth.py`

### Error: "Email not verified"
**Solution**: Make sure your Google account email is verified

### Error: "Missing client_id"
**Solution**: Check that your `.env.web` file has the correct OAuth credentials set

---

## Security Notes

✅ **What's Secure:**
- Only Google-verified emails can log in
- Only specific domains allowed
- No passwords stored
- OAuth tokens handled by Google
- Session-based authentication

⚠️ **Important:**
- Never commit `.env.web` to git (already in .gitignore)
- Keep your OAuth Client Secret private
- For production, use HTTPS only (Render provides this automatically)

---

## Testing Checklist

- [ ] OAuth credentials created in Google Cloud Console
- [ ] `.env.web` updated with Client ID and Secret
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] App starts without errors
- [ ] Login page shows "Sign in with Google" button
- [ ] Clicking button redirects to Google
- [ ] Can log in with @digidom.ventures email
- [ ] Can log in with @banyantreedigital.com email
- [ ] Cannot log in with other domains (gmail.com, etc.)
- [ ] After login, redirected to dashboard
- [ ] Logout works correctly

---

## Quick Reference

**Local redirect URI**: `http://localhost:5000/authorize`
**Production redirect URI**: `https://your-app.onrender.com/authorize`

**Allowed domains**:
- @digidom.ventures
- @banyantreedigital.com

**Required .env variables**:
- `GOOGLE_OAUTH_CLIENT_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET`
