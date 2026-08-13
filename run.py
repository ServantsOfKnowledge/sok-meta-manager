#!/usr/bin/env python3
"""
SOK MetaManager — Launcher  (v2)
Servants of Knowledge · IA Metadata + Transliteration Tool

Usage:
  python3 run.py           # start the app (opens browser automatically)
  python3 run.py --no-browser   # start without opening browser
"""
import os
import sys
import subprocess
import webbrowser
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).parent
PORT = 5050
URL  = f"http://127.0.0.1:{PORT}"


def check_deps():
    """Check required and optional Python packages."""
    missing_required = []
    for pkg, imp in [
        ("flask",            "flask"),
        ("internetarchive",  "internetarchive"),
        ("requests",         "requests"),
    ]:
        try:
            __import__(imp)
        except ImportError:
            missing_required.append(pkg)

    if missing_required:
        print(f"\n⚠  Missing required packages: {', '.join(missing_required)}")
        print("   Run:  pip3 install " + " ".join(missing_required))
        print("   Or:   pip3 install -r requirements.txt")
        sys.exit(1)
    else:
        print("   Core packages:   ✓")

    # Optional — transliteration engine
    try:
        import indic_transliteration  # noqa
        print("   Transliteration: ✓ indic-transliteration available")
    except ImportError:
        print("   Transliteration: ✗ not installed (optional — needed for script conversion)")
        print("     To enable:  pip3 install indic-transliteration")


def check_ia_config():
    result = subprocess.run(["ia", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("\n⚠  ia CLI not found.")
        print("   Install:  pip3 install internetarchive")
        print("   Config:   ia configure")
    else:
        print(f"   ia CLI:          ✓ {result.stdout.strip()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SOK MetaManager")
    parser.add_argument("--no-browser", action="store_true",
                        help="Do not open browser automatically")
    parser.add_argument("--port", type=int, default=PORT,
                        help=f"Port to listen on (default: {PORT})")
    args = parser.parse_args()
    PORT = args.port
    URL  = f"http://127.0.0.1:{PORT}"

    print("\n🕉  SOK MetaManager — Servants of Knowledge")
    print("   Internet Archive Metadata + Transliteration Tool  v2")
    print("─" * 56)

    check_deps()
    check_ia_config()

    # Initialize database
    sys.path.insert(0, str(ROOT))
    import database
    database.init_db()
    # Mark any jobs that were running when the app last stopped
    database.mark_interrupted_jobs()
    print(f"   Database:        {database.DB_PATH}")
    print(f"   Collection DBs:  {database.COLL_DB_DIR}")

    if not args.no_browser:
        def open_browser():
            time.sleep(1.4)
            webbrowser.open(URL)
        import threading
        threading.Thread(target=open_browser, daemon=True).start()

    print(f"\n   Running at {URL}")
    print("   Press Ctrl+C to stop\n")

    from app import app, start_job_worker
    start_job_worker()   # background job queue (sync, pushes, bulk ops)
    app.run(host="127.0.0.1", port=PORT, debug=False, use_reloader=False)
