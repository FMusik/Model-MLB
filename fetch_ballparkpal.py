import requests
import datetime
import os
import sys
import pandas as pd

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

if "sign out" not in login.text.lower():
    print("❌ Login failed")
    sys.exit(1)

print("✅ Logged in!")
print(f"📥 Downloading games for {DATE}...")
response = session.get(EXPORT_URL)

with open("ballparkpal_games.xlsx", "wb") as f:
    f.write(response.content)

print(f"✅ Saved ballparkpal_games.xlsx ({len(response.content):,} bytes)")

# DEBUG — print exactly what's in the file
print("\n🔍 DEBUG — BP file contents:")
df = pd.read_excel("ballparkpal_games.xlsx", engine="openpyxl")
print(f"  Columns: {list(df.columns)}")
print(f"  Rows: {len(df)}")
print("\n  First 3 rows:")
for _, row in df.head(3).iterrows():
    print(f"    Away={row.get('AwayTeam')} Home={row.get('HomeTeam')} Runs={row.get('RunsAway')}/{row.get('RunsHome')}")
