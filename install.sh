#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  SOK MetaManager — First-time Install Script
#  Servants of Knowledge · Internet Archive Metadata Tool
#
#  Usage:
#    chmod +x install.sh
#    ./install.sh
# ─────────────────────────────────────────────────────────────
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "🕉  SOK MetaManager — Install"
echo "──────────────────────────────────────────────────"

# ── Python check ─────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "❌  Python 3 not found. Please install Python 3.9+ first."
    exit 1
fi
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "   Python:  $PY_VER ✓"

# ── pip install required packages ────────────────────────────
echo ""
echo "   Installing Python packages from requirements.txt…"
pip3 install -r requirements.txt

echo ""
echo "   Installing optional transliteration engine…"
pip3 install indic-transliteration || echo "   (indic-transliteration failed — transliteration features will be disabled)"

# ── ia CLI check & configure ─────────────────────────────────
echo ""
if command -v ia &>/dev/null; then
    echo "   ia CLI: $(ia --version 2>&1) ✓"
else
    echo "   ia CLI not found — installing via pip…"
    pip3 install internetarchive
fi

echo ""
echo "   Checking ia CLI configuration…"
if ! ia list &>/dev/null 2>&1; then
    echo ""
    echo "   ⚠  ia CLI is not yet configured with your IA credentials."
    echo "   Please run:  ia configure"
    echo "   (You will be prompted for your Archive.org email and password.)"
else
    echo "   ia CLI configured ✓"
fi

# ── Create data directory ─────────────────────────────────────
mkdir -p data
echo "   Data directory: $SCRIPT_DIR/data ✓"

# ── Initialize DB ─────────────────────────────────────────────
echo ""
echo "   Initialising database…"
python3 -c "import database; database.init_db(); print('   Database initialised ✓')"

echo ""
echo "──────────────────────────────────────────────────"
echo "✅  Install complete!"
echo ""
echo "   Start the app:   python3 run.py"
echo "   Open browser to: http://127.0.0.1:5050"
echo ""
