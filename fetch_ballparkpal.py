"""
fetch_ballparkpal.py
Logs into BallparkPal and downloads today's game export.
Saves as ballparkpal_games.xlsx in the current directory.
"""
import requests
import datetime
import os
import sys

EMAIL    = os.environ["BP_EMAIL"]
PASSWORD = os.environ["BP_PASSWORD"]
DATE     = datetime.date.today().strftime("%Y-%m-%d")

LOGIN_URL  = "https://www.ballparkpal.com/login.php"
EXPORT_URL = f"https://www.ballparkpal.com/ExportGames.php?date={DATE}"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

print(f"🔐 Logging into BallparkPal...")
login = session.post(LOGIN_URL, data={
    "email":    EMAIL,
    "password": PASSWORD
})

if "Sign out" not in login.text and "sign-out" not in login.text.lower():
    print("❌ Login failed — check BP_EMAIL and BP_PASSWORD secrets")
    sys.exit(1)

print(f"✅ Logged in!")
print(f"📥 Downloading games for {DATE}...")

response = session.get(EXPORT_URL)

if response.status_code != 200:
    print(f"❌ Download failed: {response.status_code}")
    sys.exit(1)

with open("ballparkpal_games.xlsx", "wb") as f:
    f.write(response.content)

print(f"✅ Saved ballparkpal_games.xlsx ({len(response.content):,} bytes)")
