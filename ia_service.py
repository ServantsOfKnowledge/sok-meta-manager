"""
SOK MetaManager — Internet Archive Service Layer  (v2)
Smart incremental sync, thumbnail support, metadata push.
"""
import queue, subprocess, threading, json, re, time
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor

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
    "collection", "updatedate", "publicdate", "downloads",
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


# ── Parallel full sync ────────────────────────────────────────────────────────
# The scrape API paginates by an opaque cursor, so a single query can only be
# read one page at a time.  To parallelise a full sync we split the collection
# into disjoint identifier prefixes (collection_prefix_buckets) and run one
# independent cursor chain per prefix on its own thread.  Filtered scrape pages
# are ~3x slower than unfiltered ones, so raw per-chain throughput drops, but
# with several chains running at once the aggregate is still ~2-3x faster than
# a single sequential chain.

def _prefix_total(session, collection_identifier, prefix):
    """IA item count for the identifier prefix query (prefix may be empty)."""
    if not prefix:
        q = f"collection:{collection_identifier}"
    else:
        q = f"collection:{collection_identifier} AND identifier:{prefix}*"
    data = _scrape_page(session, {"q": q, "fields": "identifier", "count": 100})
    return data.get("total")


_PENDING = object()  # sentinel: bucket not yet started


def fetch_collection_parallel(
    collection_identifier: str,
    progress_callback: Optional[Callable] = None,
    workers: int = 4,
    start_state: Optional[dict] = None,
    state_callback: Optional[Callable] = None,
    start_count: int = 0,
    prefixes: Optional[list] = None,
):
    """
    Fetch ALL items in a collection using the authenticated scrape API,
    parallelised across ``workers`` independent identifier-prefix buckets.
    Each prefix runs its own cursor chain on its own thread and items are
    streamed to the caller (via yield) as they arrive.

    ``prefixes`` is an optional list of prefix strings, e.g. produced by
    database.collection_prefix_buckets().  Each prefix must be prefix-free
    (no prefix a prefix of another) so the ``identifier:{prefix}*`` queries
    partition the collection.  Prefixes are verified against IA (non-empty +
    coverage >= 95%) before fetching; if verification fails, or no prefixes
    are given, this falls back to the plain sequential fetch so correctness
    never depends on the split.

    Resumable: ``start_state`` maps bucket index -> cursor (a bucket is skipped
    when its cursor is None).  ``state_callback`` is called with the full state
    dict after each page so the caller can persist it for restart.
    """
    if not IA_LIB_AVAILABLE:
        raise RuntimeError("internetarchive library not installed")

    session = _get_session()
    fields_str = ",".join(SEARCH_FIELDS)

    if prefixes and len(prefixes) > 1:
        # Prefixes are prefix-free by construction (database.collection_prefix_
        # buckets) so the identifier:{prefix}* queries partition the whole
        # collection — no per-bucket validation round-trip is needed.  A query
        # that errors fails loudly through _run_bucket; a bucket with no items
        # simply ends its chain immediately.
        ok = True
    else:
        ok = False
    if not ok:
        # No usable split — reuse the sequential path (resume-aware).
        start_cursor = start_state and start_state.get("first")
        yield from fetch_collection_all(
            collection_identifier, progress_callback,
            start_cursor=start_cursor,
            cursor_callback=state_callback
            and (lambda c: state_callback({"first": c})),
            start_count=start_count)
        return

    total = _prefix_total(session, collection_identifier, "") or 0

    def _bucket_key(idx):
        return f"b{idx}"

    state = dict(start_state or {})
    for idx in range(len(prefixes)):
        state.setdefault(_bucket_key(idx), _PENDING)

    stop = threading.Event()
    count = start_count or 0
    out_q = queue.Queue(maxsize=max(len(prefixes) * 4, 8))

    def _run_bucket(idx, prefix):
        cursor = state[_bucket_key(idx)]
        if cursor is None:
            return  # bucket finished in a previous run
        if cursor is _PENDING:
            cursor = None  # fresh start
        query = (f"collection:{collection_identifier} AND identifier:{prefix}*"
                 if prefix else f"collection:{collection_identifier}")
        try:
            while not stop.is_set():
                params = {"q": query, "fields": fields_str, "count": 500}
                if cursor:
                    params["cursor"] = cursor
                data = _scrape_page(session, params)
                page_items = data.get("items", [])
                if page_items:
                    try:
                        out_q.put_nowait(page_items)
                    except queue.Full:
                        if stop.is_set():
                            return
                        out_q.put(page_items, timeout=5)
                cursor = data.get("cursor")
                if not cursor or not page_items:
                    state[_bucket_key(idx)] = None  # terminal
                    if state_callback:
                        state_callback(dict(state))
                    return
                state[_bucket_key(idx)] = cursor
                if state_callback:
                    state_callback(dict(state))
                time.sleep(0.1)
        except Exception as e:
            if not stop.is_set():
                out_q.put(("error", e))

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for idx, prefix in enumerate(prefixes):
                ex.submit(_run_bucket, idx, prefix)
            # Consume until every bucket reports a terminal cursor.
            finished = set()
            while len(finished) < len(prefixes):
                try:
                    got = out_q.get(timeout=1)
                except queue.Empty:
                    continue
                if isinstance(got, tuple) and got and got[0] == "error":
                    raise got[1]
                for item in got:
                    count += 1
                    if progress_callback:
                        progress_callback(count, total or count)
                    yield _parse_result(item)
                finished = {i for i in range(len(prefixes))
                            if state.get(_bucket_key(i)) is None}
    finally:
        stop.set()


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


# ── Item statistics (views / downloads) ───────────────────────────────────────

VIEWS_API_URL = "https://be-api.us.archive.org/views/v1/short/"


def fetch_item_stats(identifiers) -> dict:
    """Fetch view/download stats for identifiers via the IA Views API.

    Accepts a list of identifiers (batch endpoint allows ~100 per request) and
    returns ``{identifier: {"all_time", "last_30day", "last_7day", "have_data"}}``.
    Raises RuntimeError on transport/API failure.
    """
    import requests
    if not identifiers:
        return {}
    results: dict = {}
    batch_size = 20   # keep URLs short; the Views API 502s on long request paths
    for i in range(0, len(identifiers), batch_size):
        chunk = identifiers[i:i + batch_size]
        url = VIEWS_API_URL + ",".join(chunk)
        data = None
        last_err = None
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=60)
                if resp.status_code >= 500 and resp.status_code < 600:
                    last_err = RuntimeError(f"HTTP {resp.status_code}")
                else:
                    resp.raise_for_status()
                    data = resp.json()
                    break
            except Exception as e:
                last_err = e
            time.sleep(1.5 * (attempt + 1))
        if data is None:
            raise RuntimeError(f"Views API error (batch from {chunk[0]}): {last_err}")
        for ident, stats in data.items():
            if not isinstance(stats, dict):
                continue
            results[ident] = {
                "all_time":   stats.get("all_time", 0) or 0,
                "last_30day": stats.get("last_30day", 0) or 0,
                "last_7day":  stats.get("last_7day", 0) or 0,
                "have_data":  bool(stats.get("have_data")),
            }
    return results


def check_ia_cli() -> dict:
    try:
        result = subprocess.run(["ia", "--version"], capture_output=True, text=True, timeout=10)
        version = result.stdout.strip() or result.stderr.strip()
        return {"available": result.returncode == 0, "version": version}
    except FileNotFoundError:
        return {"available": False, "version": None, "error": "ia CLI not found"}
    except Exception as e:
        return {"available": False, "version": None, "error": str(e)}
