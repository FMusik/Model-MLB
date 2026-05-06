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
BATTERS_URL  = f"https://www.ballparkpal.com/ExportBatters.php?date={DATE}"

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

XLSX_MAGIC = b"PK\x03\x04"  # xlsx is a zip archive — must start with this signature

failures = []
for url, filename in [
    (GAMES_URL,    "ballparkpal_games.xlsx"),
    (PITCHERS_URL, "ballparkpal_pitchers.xlsx"),
    (TEAMS_URL,    "ballparkpal_teams.xlsx"),
    (BATTERS_URL,  "ballparkpal_batters.xlsx"),
]:
    print(f"📥 Downloading {filename}...")
    r = session.get(url)
    if r.status_code != 200:
        msg = f"HTTP {r.status_code}"
        print(f"❌ Failed {filename}: {msg}")
        failures.append((filename, msg))
        continue

    body = r.content
    ctype = (r.headers.get("Content-Type") or "").lower()

    if not body.startswith(XLSX_MAGIC):
        preview = body[:200].decode("utf-8", errors="replace").strip().replace("\n", " ")
        msg = (
            f"not an xlsx — got Content-Type={ctype!r}, "
            f"size={len(body):,} bytes, body starts with: {preview!r}"
        )
        print(f"❌ Failed {filename}: {msg}")
        failures.append((filename, msg))
        continue

    with open(filename, "wb") as f:
        f.write(body)
    print(f"✅ Saved {filename} ({len(body):,} bytes, Content-Type={ctype})")

if failures:
    print(f"\n❌ {len(failures)} download(s) failed:")
    for fname, msg in failures:
        print(f"   - {fname}: {msg}")
    sys.exit(1)

import pandas as pd

print("\n🔍 PITCHERS columns:")
df_p = pd.read_excel("ballparkpal_pitchers.xlsx", engine="openpyxl")
print(f"  {list(df_p.columns)}")
if len(df_p) > 0:
    print("  First row sample:")
    print(f"  {df_p.iloc[0].to_dict()}")
else:
    print("  ⚠️  Pitchers file has no data rows")

print("\n🔍 TEAMS columns:")
df_t = pd.read_excel("ballparkpal_teams.xlsx", engine="openpyxl")
print(f"  {list(df_t.columns)}")
if len(df_t) > 0:
    print("  First row sample:")
    print(f"  {df_t.iloc[0].to_dict()}")
else:
    print("  ⚠️  Teams file has no data rows")
