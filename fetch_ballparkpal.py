import requests
import datetime
import os
import sys

EMAIL    = os.environ["BP_EMAIL"]
PASSWORD = os.environ["BP_PASSWORD"]
DATE     = datetime.date.today().strftime("%Y-%m-%d")

LOGIN_URL    = "https://www.ballparkpal.com/LogIn.php"
GAMES_URL    = f"https://www.ballparkpal.com/ExportGames.php?date={DATE}"
PITCHERS_URL = f"https://www.ballparkpal.com/ExportPitchers.php?date={DATE}"
TEAMS_URL    = f"https://www.ballparkpal.com/ExportTeams.php?date={DATE}"

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

if "sign out" not in login.text.lower():
    print("❌ Login failed")
    sys.exit(1)

print("✅ Logged in!")

for url, filename in [
    (GAMES_URL,    "ballparkpal_games.xlsx"),
    (PITCHERS_URL, "ballparkpal_pitchers.xlsx"),
    (TEAMS_URL,    "ballparkpal_teams.xlsx"),
]:
    print(f"📥 Downloading {filename}...")
    r = session.get(url)
    if r.status_code == 200:
        with open(filename, "wb") as f:
            f.write(r.content)
        print(f"✅ Saved {filename} ({len(r.content):,} bytes)")
    else:
        print(f"⚠️  Failed {filename}: {r.status_code}")
