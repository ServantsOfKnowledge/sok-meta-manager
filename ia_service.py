"""
SOK IA Metadata Manager — Internet Archive Service Layer
Wraps `internetarchive` Python library + ia CLI for sync and push operations.
"""
import subprocess
import json
import re
from typing import Optional

try:
    import internetarchive as ia
    IA_LIB_AVAILABLE = True
except ImportError:
    IA_LIB_AVAILABLE = False


def _coerce_str(val):
    """Flatten list/tuple values from IA metadata into a single string."""
    if isinstance(val, (list, tuple)):
        return " | ".join(str(v) for v in val)
    return val


def _extract_year(date_str):
    """Try to extract a 4-digit year from a date string."""
    if not date_str:
        return None
    m = re.search(r"\b(\d{4})\b", str(date_str))
    return m.group(1) if m else None


def fetch_collection_items(collection_identifier: str,
                           progress_callback=None) -> list[dict]:
    """
    Fetch all items in a collection from Internet Archive.
    Returns a list of metadata dicts.
    """
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    query = f"collection:{collection_identifier}"
    fields = [
        "identifier", "title", "creator", "author", "publisher",
        "date", "year", "language", "subject", "description",
        "licenseurl", "mediatype", "volume", "isbn", "source",
        "collection"
    ]

    results = []
    search = ia.search_items(query, fields=fields)
    total = search.num_found

    for i, result in enumerate(search):
        item_meta = {}
        for field in fields:
            val = result.get(field)
            if val is not None:
                item_meta[field] = _coerce_str(val)

        # Pull collections list separately
        coll_val = result.get("collection", [])
        if isinstance(coll_val, str):
            coll_val = [coll_val]
        item_meta["_collections"] = coll_val or []

        # Derive year from date if not present
        if not item_meta.get("year") and item_meta.get("date"):
            item_meta["year"] = _extract_year(item_meta["date"])

        results.append(item_meta)

        if progress_callback:
            progress_callback(i + 1, total)

    return results


def fetch_item_metadata(identifier: str) -> Optional[dict]:
    """Fetch full metadata for a single IA item."""
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")
    try:
        item = ia.get_item(identifier)
        meta = dict(item.metadata)
        coll = meta.get("collection", [])
        if isinstance(coll, str):
            coll = [coll]
        meta["_collections"] = coll
        return meta
    except Exception as e:
        return {"error": str(e)}


def push_metadata_to_ia(identifier: str, fields: dict) -> dict:
    """
    Push updated metadata fields to Internet Archive using ia CLI.
    Returns {"success": bool, "output": str, "error": str}.
    """
    # Build ia metadata modify command
    cmd = ["ia", "metadata", identifier]

    for key, value in fields.items():
        if key.startswith("_") or key in ("identifier", "ia_raw", "extra_metadata"):
            continue
        if value is not None and str(value).strip():
            cmd += ["--modify", f"{key}:{value}"]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            return {"success": True, "output": result.stdout.strip(), "error": ""}
        else:
            return {"success": False, "output": result.stdout.strip(),
                    "error": result.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"success": False, "output": "", "error": "Command timed out after 60s"}
    except FileNotFoundError:
        return {"success": False, "output": "",
                "error": "ia CLI not found. Make sure it is installed and on PATH."}
    except Exception as e:
        return {"success": False, "output": "", "error": str(e)}


def add_to_collection(identifier: str, collection: str) -> dict:
    """Add an item to a collection via ia CLI."""
    return push_metadata_to_ia(identifier, {"collection": collection})


def remove_from_collection(identifier: str, collection: str) -> dict:
    """Remove an item from a collection via ia CLI metadata remove."""
    cmd = ["ia", "metadata", identifier, "--remove", f"collection:{collection}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return {"success": True, "output": result.stdout.strip(), "error": ""}
        else:
            return {"success": False, "output": result.stdout.strip(),
                    "error": result.stderr.strip()}
    except Exception as e:
        return {"success": False, "output": "", "error": str(e)}


def check_ia_cli() -> dict:
    """Check whether ia CLI is available and configured."""
    try:
        result = subprocess.run(
            ["ia", "--version"], capture_output=True, text=True, timeout=10
        )
        version = result.stdout.strip() or result.stderr.strip()
        return {"available": result.returncode == 0, "version": version}
    except FileNotFoundError:
        return {"available": False, "version": None,
                "error": "ia CLI not found on PATH"}
    except Exception as e:
        return {"available": False, "version": None, "error": str(e)}
