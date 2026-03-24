#!/bin/bash
# Run this once from inside the llobregat-twin/ folder on your Mac terminal:
#   cd ~/Documents/Llobregat_Viz/llobregat-twin
#   bash init_git.sh
#
# This initializes git, stages everything, and makes the first commit.
# After this you can push to GitHub as normal.

set -e

echo "Removing any stale .git directory (if present from a failed init)..."
rm -rf .git

echo "Initializing git..."
git init
git branch -m main

echo "Staging all files..."
git add .

echo "Creating initial commit..."
git commit -m "feat: Phase 1 scaffold — full project structure, fetchers, tests, and dashboard skeleton

- Full directory structure per spec (docs/, app/, data/, config/, tests/)
- docs/: INSTRUCTIONS.md, DATA_SCHEMA.md, DECISIONS.md, PROJECT_MEMORY.md
- config/: thresholds.yaml and station_metadata.yaml (provisional station IDs)
- data/fetchers/: aca.py, aemet.py, utils.py, refresh_all.py
- app/: Streamlit multi-page skeleton — app.py + 5 pages
- tests/: test_aca_fetcher.py, test_aemet_fetcher.py, test_data_schema.py
- .github/workflows/refresh_data.yml: 30-min cron + manual trigger
- .gitignore, README.md, requirements.txt"

echo ""
echo "Done! To push to GitHub:"
echo "  git remote add origin https://github.com/YOUR_USERNAME/llobregat-twin.git"
echo "  git push -u origin main"
