name: Autoscore Tracker (v3)

on:
  workflow_dispatch:        # manual trigger (also used for backfills)
  schedule:
    # 12:00 UTC ≈ 8:00 AM ET — grade yesterday's finals before the new slate
    - cron: "0 12 * * *"

jobs:
  autoscore:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Write Google credentials
        run: |
          cat > credentials.json << 'JSON'
          ${{ secrets.GOOGLE_CREDENTIALS }}
          JSON

      - name: Grade pending Tracker bets
        # SHEET_ID = the v3 Google Sheet's ID (the part of the URL between /d/ and /edit).
        # The name "MLB Model" also works via the open()-by-name fallback.
        run: python mlb_autoscorer.py "${{ secrets.SHEET_ID }}"

      - name: Clean up credentials
        if: always()
        run: rm -f credentials.json
