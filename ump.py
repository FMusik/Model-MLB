name: Run MLB Model (v3)

on:
  workflow_dispatch:        # manual "Run workflow" button
  schedule:
    # 14:00 UTC ≈ 10:00 AM ET — adjust to taste (cron is UTC)
    - cron: "0 14 * * *"

jobs:
  run-model:
    runs-on: ubuntu-latest
    timeout-minutes: 30
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

      - name: Run model
        env:
          ODDS_API_KEY: ${{ secrets.ODDS_API_KEY }}
          ODDSPAPI_KEY: ${{ secrets.ODDSPAPI_KEY }}
        run: python main.py

      - name: Clean up credentials
        if: always()
        run: rm -f credentials.json
