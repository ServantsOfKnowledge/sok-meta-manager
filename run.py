#!/usr/bin/env python3
"""
SOK IA Metadata Manager — Launcher
Run:  python run.py
"""
import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path

ROOT = Path(__file__).parent
PORT = 5050
URL  = f"http://127.0.0.1:{PORT}"


def check_deps():
    missing = []
    try:
        import flask
    except ImportError:
        missing.append("flask")
    try:
        import internetarchive
    except ImportError:
        missing.append("internetarchive")
    if missing:
        print(f"\n⚠  Missing packages: {', '.join(missing)}")
        print("   Run:  pip install " + " ".join(missing))
        sys.exit(1)


def check_ia_config():
    result = subprocess.run(["ia", "--version"],
                            capture_output=True, text=True)
    if result.returncode != 0:
        print("\n⚠  ia CLI not found. Install with:  pip install internetarchive")
        print("   Then configure:  ia configure")
    else:
        print(f"   ia CLI: {result.stdout.strip()}")


if __name__ == "__main__":
    print("\n🕉  SOK IA Metadata Manager")
    print("   Servants of Knowledge · Internet Archive Tool")
    print("─" * 48)

    check_deps()
    check_ia_config()

    # Initialize database
    sys.path.insert(0, str(ROOT))
    import database
    database.init_db()
    print(f"   DB:   {database.DB_PATH}")

    # Open browser after short delay
    def open_browser():
        time.sleep(1.4)
        webbrowser.open(URL)

    import threading
    threading.Thread(target=open_browser, daemon=True).start()

    print(f"\n   Running at {URL}")
    print("   Press Ctrl+C to stop\n")

    from app import app
    app.run(host="127.0.0.1", port=PORT, debug=False, use_reloader=False)
