"""
SOK MetaManager — Internet Archive Service Layer  (v2)
Smart incremental sync, thumbnail support, metadata push.
"""
import subprocess, json, re, time
from typing import Optional, Callable

try:
    import internetarchive as ia
    IA_LIB_AVAILABLE = True
except ImportError:
    IA_LIB_AVAILABLE = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _coerce(val):
    if isinstance(val, (list, tuple)):
        return " | ".join(str(v) for v in val)
    return val

def _year(date_str):
    if not date_str:
        return None
    m = re.search(r"\b(\d{4})\b", str(date_str))
    return m.group(1) if m else None


# ── Thumbnail ─────────────────────────────────────────────────────────────────

def thumbnail_url(identifier: str) -> str:
    """Return the IA thumbnail URL for an identifier (always valid URL)."""
    return f"https://archive.org/services/img/{identifier}"


# ── Full collection sync ──────────────────────────────────────────────────────

SEARCH_FIELDS = [
    "identifier", "title", "creator", "author", "publisher",
    "date", "year", "language", "subject", "description",
    "licenseurl", "mediatype", "volume", "isbn", "source",
    "collection", "updatedate", "publicdate",
]

def fetch_collection_items(
    collection_identifier: str,
    progress_callback: Optional[Callable] = None,
    start_page: Optional[int] = None,
    page_callback: Optional[Callable] = None,
    start_count: int = 0,
):
    """Fetch ALL items in a collection (full sync, public search API).  Streams
    each item via yield as it is retrieved so the caller can persist records
    incrementally.  Resumable: pass a previously saved ``start_page`` to
    continue where a previous run left off; ``page_callback`` receives the next
    page number after each page so the caller can persist it."""
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    query = f"collection:{collection_identifier}"
    page  = int(start_page or 1)
    i     = start_count or 0

    while True:
        search = ia.search_items(
            query, fields=SEARCH_FIELDS, params={"page": page, "rows": 500}
        )
        total = search.num_found
        n_items = 0
        for result in search.iter_as_results():
            i += 1
            n_items += 1
            if progress_callback:
                progress_callback(i, total or i)
            time.sleep(0.1)   # polite rate limiting
            yield _parse_result(result)

        if n_items == 0:
            break
        if page_callback:
            page_callback(page + 1)
        page += 1


def _scrape_page(session, params, timeout=90, attempts=4):
    """GET one page of the IA scrape API, retrying transient network/DNS
    errors with backoff so a single blip doesn't abort an entire sync."""
    last_err = None
    for attempt in range(attempts):
        try:
            resp = session.get(
                "https://archive.org/services/search/v1/scrape",
                params=params,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Scrape API error: {last_err}")


def fetch_updated_items(
    collection_identifier: str,
    since_date: Optional[str],
    progress_callback: Optional[Callable] = None,
    start_cursor: Optional[str] = None,
    cursor_callback: Optional[Callable] = None,
    start_count: int = 0,
):
    """
    Smart incremental sync: fetch only items updated on IA after since_date.
    since_date should be an ISO date string like "2024-01-15T10:30:00".
    Falls back to full sync if since_date is None.

    Uses the authenticated scrape API (same as fetch_collection_all) so that
    hidden/noindex items are included and authentication errors surface clearly.
    Streams each item via yield as it is retrieved so the caller can persist
    records incrementally (portal updates on the fly).  Resumable: pass a
    previously saved ``start_cursor`` to continue where a previous run left
    off; ``cursor_callback`` is invoked with the next cursor after each page.
    """
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    if since_date:
        # Trim to date portion; IA Lucene expects YYYY-MM-DD and uses * for open end
        date_part = since_date[:10]
        query = (
            f"collection:{collection_identifier} "
            f"AND updatedate:[{date_part} TO *]"
        )
    else:
        query = f"collection:{collection_identifier}"

    session = _get_session()
    cursor = start_cursor
    total  = None
    count  = start_count or 0
    fields_str = ",".join(SEARCH_FIELDS)

    while True:
        params: dict = {
            "q":      query,
            "fields": fields_str,
            "count":  500,
        }
        if cursor:
            params["cursor"] = cursor

        data = _scrape_page(session, params)

        if total is None:
            total = data.get("total", 0)

        page_items = data.get("items", [])
        for item in page_items:
            count += 1
            if progress_callback:
                progress_callback(count, total or count)
            yield _parse_result(item)

        cursor = data.get("cursor")
        if cursor_callback and cursor:
            cursor_callback(cursor)

        if not cursor or not page_items:
            break

        time.sleep(0.1)


def _parse_result(result: dict) -> dict:
    item_meta = {}
    for field in SEARCH_FIELDS:
        val = result.get(field)
        if val is not None:
            item_meta[field] = _coerce(val)

    coll_val = result.get("collection", [])
    if isinstance(coll_val, str):
        coll_val = [coll_val]
    item_meta["_collections"] = coll_val or []

    if not item_meta.get("year") and item_meta.get("date"):
        item_meta["year"] = _year(item_meta["date"])

    return item_meta


# ── Authenticated full sync (includes noindex / hidden items) ─────────────────

def _get_session():
    """Return an authenticated internetarchive Session."""
    return ia.get_session()


def fetch_collection_all(
    collection_identifier: str,
    progress_callback: Optional[Callable] = None,
    start_cursor: Optional[str] = None,
    cursor_callback: Optional[Callable] = None,
    start_count: int = 0,
):
    """
    Fetch ALL items in a collection using IA's authenticated scrape API.
    Unlike the regular search, this includes noindex/hidden items visible
    to the logged-in account (requires `ia configure` with valid credentials).
    Streams each item via yield as it is retrieved so the caller can persist
    records incrementally (portal updates on the fly).  Resumable: pass a
    previously saved ``start_cursor`` to continue where a previous run left
    off; ``cursor_callback`` is invoked with the next cursor after each page.
    """
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    session = _get_session()
    cursor = start_cursor
    total  = None
    count  = start_count or 0
    fields_str = ",".join(SEARCH_FIELDS)

    while True:
        params = {
            "q": f"collection:{collection_identifier}",
            "fields": fields_str,
            "count": 500,
        }
        if cursor:
            params["cursor"] = cursor

        data = _scrape_page(session, params)

        if total is None:
            total = data.get("total", 0)

        page_items = data.get("items", [])
        for item in page_items:
            count += 1
            if progress_callback:
                progress_callback(count, total or count)
            yield _parse_result(item)

        cursor = data.get("cursor")
        if cursor_callback and cursor:
            cursor_callback(cursor)

        if not cursor or not page_items:
            break

        time.sleep(0.1)


# ── Sub-collection discovery ──────────────────────────────────────────────────

def fetch_sub_collections(collection_identifier: str) -> list:
    """
    Discover sub-collections nested inside a super-collection.
    Uses the authenticated scrape API so hidden sub-collections are found too.
    Returns a list of dicts: {ia_id, name}
    """
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    session = _get_session()
    results, cursor = [], None

    while True:
        params = {
            "q": f"collection:{collection_identifier} AND mediatype:collection",
            "fields": "identifier,title",
            "count": 500,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            resp = session.get(
                "https://archive.org/services/search/v1/scrape",
                params=params,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            # Fall back to regular search if scrape fails
            query  = f"collection:{collection_identifier} AND mediatype:collection"
            search = ia.search_items(query, fields=["identifier", "title"])
            for result in search:
                ia_id = result.get("identifier", "").strip()
                name  = result.get("title") or ia_id
                if ia_id and ia_id != collection_identifier:
                    results.append({"ia_id": ia_id, "name": _coerce(name)})
            return results

        for item in data.get("items", []):
            ia_id = item.get("identifier", "").strip()
            name  = item.get("title") or ia_id
            if ia_id and ia_id != collection_identifier:
                results.append({"ia_id": ia_id, "name": _coerce(name)})

        cursor = data.get("cursor")
        if not cursor or not data.get("items"):
            break

    return results


# ── Single item ───────────────────────────────────────────────────────────────

def fetch_item_metadata(identifier: str) -> Optional[dict]:
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


# ── Push metadata ─────────────────────────────────────────────────────────────

def push_metadata_to_ia(identifier: str, fields: dict) -> dict:
    cmd = ["ia", "metadata", identifier]
    for key, value in fields.items():
        if key.startswith("_") or key in ("identifier", "ia_raw", "extra_metadata"):
            continue
        if value is not None and str(value).strip():
            cmd += ["--modify", f"{key}:{value}"]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return {"success": True, "output": result.stdout.strip(), "error": ""}
        return {"success": False, "output": result.stdout.strip(), "error": result.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"success": False, "output": "", "error": "Timed out after 60s"}
    except FileNotFoundError:
        return {"success": False, "output": "", "error": "ia CLI not found. Run: pip install internetarchive"}
    except Exception as e:
        return {"success": False, "output": "", "error": str(e)}


def add_to_collection(identifier: str, collection: str) -> dict:
    return push_metadata_to_ia(identifier, {"collection": collection})


def remove_from_collection(identifier: str, collection: str) -> dict:
    cmd = ["ia", "metadata", identifier, "--remove", f"collection:{collection}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return {"success": result.returncode == 0,
                "output": result.stdout.strip(), "error": result.stderr.strip()}
    except Exception as e:
        return {"success": False, "output": "", "error": str(e)}


def check_ia_cli() -> dict:
    try:
        result = subprocess.run(["ia", "--version"], capture_output=True, text=True, timeout=10)
        version = result.stdout.strip() or result.stderr.strip()
        return {"available": result.returncode == 0, "version": version}
    except FileNotFoundError:
        return {"available": False, "version": None, "error": "ia CLI not found"}
    except Exception as e:
        return {"available": False, "version": None, "error": str(e)}
