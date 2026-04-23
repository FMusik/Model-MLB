import requests
import datetime
import os
import sys

EMAIL    = os.environ["BP_EMAIL"]
PASSWORD = os.environ["BP_PASSWORD"]
DATE     = datetime.date.today().strftime("%Y-%m-%d")

LOGIN_URL  = "https://www.ballparkpal.com/LogIn.php"
EXPORT_URL = f"https://www.ballparkpal.com/ExportGames.php?date={DATE}"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.ballparkpal.com/LogIn.php",
})

print("🔐 Logging into BallparkPal...")
login = session.post(LOGIN_URL, data={
    "email":    EMAIL,
    "password": PASSWORD,
    "login":    "Sign In",
}, allow_redirects=True)

print(f"   Status: {login.status_code} | URL: {login.url}")

if "sign out" not in login.text.lower() and "logout" not in login.text.lower():
    print("❌ Login failed")
    print(f"   Response snippet: {login.text[500:800]}")
    sys.exit(1)

print("✅ Logged in!")
print(f"📥 Downloading games for {DATE}...")

response = session.get(EXPORT_URL)

if response.status_code != 200:
    print(f"❌ Download failed: {response.status_code}")
    sys.exit(1)

with open("ballparkpal_games.xlsx", "wb") as f:
    f.write(response.content)

print(f"✅ Saved ballparkpal_games.xlsx ({len(response.content):,} bytes)")
