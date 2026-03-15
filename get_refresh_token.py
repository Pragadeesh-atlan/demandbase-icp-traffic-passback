"""
One-time script to generate a Google OAuth2 refresh token
using a Desktop client (no OAuth Playground needed).

Usage:
    1. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in your .env file
    2. Run: python get_refresh_token.py
    3. Open the URL printed in the terminal
    4. Sign in and paste the authorization code back here
    5. Copy the refresh token into your .env file
"""

import os
import sys
import json
import urllib.parse
import urllib.request

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in your .env file first.")
    sys.exit(1)

SCOPES = "https://www.googleapis.com/auth/spreadsheets"
REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"  # Desktop / manual copy-paste flow

# Step 1: Build the authorization URL
auth_params = urllib.parse.urlencode({
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": SCOPES,
    "access_type": "offline",
    "prompt": "consent",
})
auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{auth_params}"

print("\n1. Open this URL in your browser:\n")
print(auth_url)
print("\n2. Sign in and grant access.")
print("3. Copy the authorization code and paste it below.\n")

auth_code = input("Authorization code: ").strip()

if not auth_code:
    print("ERROR: No authorization code provided.")
    sys.exit(1)

# Step 2: Exchange the authorization code for tokens
token_data = urllib.parse.urlencode({
    "code": auth_code,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "redirect_uri": REDIRECT_URI,
    "grant_type": "authorization_code",
}).encode()

req = urllib.request.Request(
    "https://oauth2.googleapis.com/token",
    data=token_data,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)

try:
    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read().decode())
except urllib.error.HTTPError as e:
    error_body = e.read().decode()
    print(f"\nERROR: Token exchange failed ({e.code}):\n{error_body}")
    sys.exit(1)

refresh_token = tokens.get("refresh_token")

if refresh_token:
    print(f"\nYour refresh token:\n\n{refresh_token}")
    print("\nAdd this to your .env file as:")
    print(f"GOOGLE_REFRESH_TOKEN={refresh_token}")
else:
    print("\nWARNING: No refresh token returned. Full response:")
    print(json.dumps(tokens, indent=2))
