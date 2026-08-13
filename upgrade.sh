#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  SOK MetaManager — Upgrade Script
#  Servants of Knowledge · Internet Archive Metadata Tool
#
#  Usage:
#    chmod +x upgrade.sh
#    ./upgrade.sh
#
#  What it does:
#    1. git pull (latest code from GitHub)
#    2. pip upgrade all packages in requirements.txt
#    3. Runs DB migration (init_db is idempotent — safe to re-run)
#    4. Reports what changed
# ─────────────────────────────────────────────────────────────
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "🕉  SOK MetaManager — Upgrade"
echo "──────────────────────────────────────────────────"

# ── Git pull ─────────────────────────────────────────────────
if [ -d ".git" ]; then
    echo "   Pulling latest code from GitHub…"
    BEFORE=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    git pull --ff-only
    AFTER=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    if [ "$BEFORE" != "$AFTER" ]; then
        echo "   Updated: $BEFORE → $AFTER"
        echo ""
        echo "   Changes:"
        git log --oneline "$BEFORE".."$AFTER" 2>/dev/null | head -20 | sed 's/^/     /'
    else
        echo "   Already up to date ($BEFORE)"
    fi
else
    echo "   ⚠  Not a git repo — skipping git pull."
    echo "   To enable auto-updates, clone from:"
    echo "   https://github.com/ServantsOfKnowledge/sok-meta-manager.git"
fi

# ── pip upgrade ───────────────────────────────────────────────
echo ""
echo "   Upgrading Python packages…"
pip3 install --upgrade -r requirements.txt

# Upgrade optional transliteration engine if already installed
if python3 -c "import indic_transliteration" &>/dev/null 2>&1; then
    echo "   Upgrading indic-transliteration…"
    pip3 install --upgrade indic-transliteration
else
    echo "   (indic-transliteration not installed — skipping)"
fi

# ── DB migration ──────────────────────────────────────────────
echo ""
echo "   Running database migration…"
python3 -c "import database; database.init_db(); print('   Database schema up to date ✓')"

echo ""
echo "──────────────────────────────────────────────────"
echo "✅  Upgrade complete!"
echo ""
echo "   Restart the app:  python3 run.py"
echo ""
