"""
SOK MetaManager — Database Layer  (v3 — per-collection sharded SQLite)

Architecture
------------
* Catalog DB  — data/sok_metadata.db
      collections, ia_sub_collections (discovered tree),
      jobs + job_log (background queue), meta (flags/migrations).
* Per-collection DB  — data/collections/<identifier>.db
      One SQLite file per collection: items, item_collections, change_log,
      review_batches and a per-collection FTS5 search index.
      Reading/writing one collection never blocks reads of another.

Item IDs are globally unique across collection files so the existing
/api/items/<id> routes keep working:
    item_id = collection_id * ID_OFFSET + local_seq

Background jobs are persisted in the catalog DB, so syncs survive app restarts
and an "Operations & Sync" page can manage them (status/logs/cancel/retry).
"""
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime

DB_PATH       = os.path.join(os.path.dirname(__file__), "data", "sok_metadata.db")
COLL_DB_DIR   = os.path.join(os.path.dirname(__file__), "data", "collections")
ID_OFFSET     = 100_000_000   # max items per collection before ids collide


# ── Catalog connection (collections / jobs / sub-collections / meta) ─────────

def get_db():
    """Open the catalog DB (collections, jobs, sub-collections, meta)."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Small in-process TTL cache ────────────────────────────────────────────────

_CACHE = {}
_CACHE_LOCK = threading.Lock()
_DEFAULT_TTL = 3.0


def cache_get(key):
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and time.time() < hit[0]:
            return hit[1]
    return None


def cache_put(key, value, ttl=_DEFAULT_TTL):
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + ttl, value)


def cache_clear():
    with _CACHE_LOCK:
        _CACHE.clear()


def _dirty_coll(collection_id):
    """Drop cached rows (stats/languages/sub-counts) that belong to a collection."""
    with _CACHE_LOCK:
        for key in list(_CACHE):
            if isinstance(key, tuple) and collection_id in key:
                _CACHE.pop(key, None)


# ── FTS5 support detection ────────────────────────────────────────────────────

_FTS_OK = None


def fts_supported():
    global _FTS_OK
    if _FTS_OK is None:
        try:
            c = sqlite3.connect(":memory:")
            c.execute("CREATE VIRTUAL TABLE _f USING fts5(x)")
            c.close()
            _FTS_OK = True
        except Exception:
            _FTS_OK = False
    return _FTS_OK


def _fts_query(q):
    """Turn a plain search string into an FTS5 MATCH expression (safe)."""
    tokens = [t for t in re.findall(r"[A-Za-z0-9]+", q.lower()) if len(t) >= 2]
    return " AND ".join('"{}"'.format(t) for t in tokens) if tokens else None


# ── Identifier / path resolution ──────────────────────────────────────────────

_IDENT_CACHE = {}


def _coll_identifier(collection_id):
    """Return the IA identifier for a collection, resolving via the catalog."""
    cached = _IDENT_CACHE.get(collection_id)
    if cached:
        return cached
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT identifier FROM collections WHERE id=?", (collection_id,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise ValueError("Collection %s not found" % collection_id)
    _IDENT_CACHE[collection_id] = row[0]
    return row[0]


def _coll_path(identifier):
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", identifier) or "collection"
    return os.path.join(COLL_DB_DIR, safe + ".db")


def _coll_id_from_item_id(item_id):
    return item_id // ID_OFFSET


# ── Per-collection DB connection ──────────────────────────────────────────────

_COLL_SCHEMAS = set()
_SCHEMA_LOCK = threading.Lock()

_COLL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS items (
    id                INTEGER PRIMARY KEY,
    identifier        TEXT    NOT NULL,
    title             TEXT,
    alt_title         TEXT,
    creator           TEXT,
    alt_creator       TEXT,
    author            TEXT,
    alt_author        TEXT,
    publisher         TEXT,
    alt_publisher     TEXT,
    date              TEXT,
    year              TEXT,
    language          TEXT,
    subject           TEXT,
    description       TEXT,
    licenseurl        TEXT,
    mediatype         TEXT,
    volume            TEXT,
    isbn              TEXT,
    source            TEXT,
    notes             TEXT,
    extra_metadata    TEXT,
    ia_raw            TEXT,
    ia_updatedate     TEXT,
    is_modified       INTEGER DEFAULT 0,
    is_pushed         INTEGER DEFAULT 0,
    last_synced       TEXT,
    last_modified     TEXT,
    last_pushed       TEXT,
    detected_language TEXT,
    translit_status   TEXT DEFAULT 'none',
    UNIQUE(identifier)
);

CREATE TABLE IF NOT EXISTS item_collections (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id    INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    collection TEXT    NOT NULL,
    UNIQUE(item_id, collection)
);

CREATE TABLE IF NOT EXISTS change_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id    INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    field      TEXT    NOT NULL,
    old_value  TEXT,
    new_value  TEXT,
    changed_at TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS review_batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER NOT NULL,
    lang_code     TEXT    NOT NULL,
    export_path   TEXT,
    exported_at   TEXT,
    imported_at   TEXT,
    item_count    INTEGER DEFAULT 0,
    status        TEXT    DEFAULT 'exported'
);

CREATE INDEX IF NOT EXISTS idx_items_identifier ON items(identifier);
CREATE INDEX IF NOT EXISTS idx_items_modified   ON items(is_modified);
CREATE INDEX IF NOT EXISTS idx_items_pushed     ON items(is_pushed);
CREATE INDEX IF NOT EXISTS idx_items_lang       ON items(detected_language);
CREATE INDEX IF NOT EXISTS idx_items_tstatus    ON items(translit_status);
CREATE INDEX IF NOT EXISTS idx_ic_collection    ON item_collections(collection);
"""


def _init_coll_conn(conn, identifier):
    """Create the per-collection schema once per process (thread-safe)."""
    with _SCHEMA_LOCK:
        if identifier in _COLL_SCHEMAS:
            return
        c = conn.cursor()
        c.executescript(_COLL_SCHEMA_SQL)
        for col, typedef in [
            ("ia_updatedate",     "TEXT"),
            ("detected_language", "TEXT"),
            ("translit_status",   "TEXT DEFAULT 'none'"),
            ("downloads",         "INTEGER DEFAULT 0"),
            ("views_30d",         "INTEGER DEFAULT 0"),
            ("views_7d",          "INTEGER DEFAULT 0"),
        ]:
            _safe_add_column(c, "items", col, typedef)
        if fts_supported():
            try:
                c.execute("CREATE VIRTUAL TABLE IF NOT EXISTS items_fts "
                          "USING fts5(identifier, title, creator, author, publisher, subject)")
            except Exception:
                pass
        conn.commit()
        _COLL_SCHEMAS.add(identifier)


def get_coll_db_conn(identifier):
    """Open (creating if needed) the per-collection DB for an identifier."""
    # Generous busy_timeout: large sync batches (which also touch the FTS
    # index) can hold the write lock for a while — let other writers wait
    # instead of failing with "database is locked".
    conn = sqlite3.connect(_coll_path(identifier), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA foreign_keys=ON")
    _init_coll_conn(conn, identifier)
    return conn


def get_coll_db(collection_id):
    """Open the per-collection DB for a collection id."""
    return get_coll_db_conn(_coll_identifier(collection_id))


# ── Schema / migration ────────────────────────────────────────────────────────

_CATALOG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS collections (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    identifier  TEXT    UNIQUE NOT NULL,
    name        TEXT    NOT NULL,
    description TEXT,
    item_count  INTEGER DEFAULT 0,
    last_synced TEXT,
    created_at  TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ia_sub_collections (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    super_id      INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    ia_id         TEXT    NOT NULL,
    name          TEXT,
    discovered_at TEXT    DEFAULT (datetime('now')),
    UNIQUE(super_id, ia_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type         TEXT NOT NULL,
    collection_id    INTEGER REFERENCES collections(id) ON DELETE SET NULL,
    title            TEXT,
    params           TEXT,
    status           TEXT DEFAULT 'queued',
    current          INTEGER DEFAULT 0,
    total            INTEGER DEFAULT 0,
    new_count        INTEGER DEFAULT 0,
    error            TEXT,
    cancel_requested INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT (datetime('now')),
    started_at       TEXT,
    finished_at      TEXT
);

CREATE TABLE IF NOT EXISTS job_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    level      TEXT DEFAULT 'info',
    message    TEXT,
    logged_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status    ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_collection ON jobs(collection_id);
CREATE INDEX IF NOT EXISTS idx_sub_super       ON ia_sub_collections(super_id);
CREATE INDEX IF NOT EXISTS idx_joblog_job      ON job_log(job_id);
"""


def _safe_add_column(c, table, col, typedef):
    try:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
    except Exception:
        pass


def _fts_rebuild_conn(conn):
    """Rebuild the FTS5 index inside an open per-collection connection."""
    if not fts_supported():
        return 0
    try:
        conn.execute("DROP TABLE IF EXISTS items_fts")
    except Exception:
        pass
    conn.execute("CREATE VIRTUAL TABLE items_fts "
                 "USING fts5(identifier, title, creator, author, publisher, subject)")
    rows = conn.execute(
        "SELECT id, identifier, title, creator, author, publisher, subject FROM items"
    ).fetchall()
    conn.executemany(
        "INSERT INTO items_fts (rowid, identifier, title, creator, author, publisher, subject) "
        "VALUES (?,?,?,?,?,?,?)",
        [tuple(r) for r in rows],
    )
    conn.commit()
    return len(rows)


def _migrate_legacy(conn):
    """One-time migration: move item data from the old single DB into per-collection DBs."""
    if conn.execute("SELECT value FROM meta WHERE key='per_coll_dbs'").fetchone():
        return
    has_items = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='items'"
    ).fetchone()[0]
    if not has_items:
        conn.execute("INSERT OR REPLACE INTO meta (key,value) VALUES ('per_coll_dbs','1')")
        conn.commit()
        return

    # Legacy tables may predate later migrations — make columns safe.
    c = conn.cursor()
    for col, typedef in [("ia_updatedate", "TEXT"), ("detected_language", "TEXT"),
                         ("translit_status", "TEXT DEFAULT 'none'")]:
        _safe_add_column(c, "items", col, typedef)
    conn.commit()

    colls = conn.execute("SELECT id, identifier FROM collections").fetchall()
    for row in colls:
        coll_id, ident = row["id"], row["identifier"]
        legacy = conn.execute(
            "SELECT * FROM items WHERE collection_id=? ORDER BY id", (coll_id,)
        ).fetchall()
        if not legacy:
            continue
        dst = get_coll_db_conn(ident)
        try:
            id_map = {}
            for idx, src in enumerate(legacy, start=1):
                new_id = coll_id * ID_OFFSET + idx
                id_map[src["id"]] = new_id
                cols = ["id"] + [k for k in src.keys() if k != "collection_id"]
                dst.execute(
                    "INSERT OR IGNORE INTO items ({}) VALUES ({})".format(
                        ", ".join(cols), ", ".join("?" * len(cols))
                    ),
                    [new_id] + [src[k] for k in cols[1:]],
                )
            for ic in conn.execute(
                """SELECT ic.item_id, ic.collection FROM item_collections ic
                   JOIN items it ON it.id = ic.item_id WHERE it.collection_id = ?""",
                (coll_id,),
            ).fetchall():
                dst.execute(
                    "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                    (id_map[ic["item_id"]], ic["collection"]),
                )
            for cl in conn.execute(
                """SELECT cl.item_id, cl.field, cl.old_value, cl.new_value, cl.changed_at
                   FROM change_log cl JOIN items it ON it.id = cl.item_id
                   WHERE it.collection_id = ?""",
                (coll_id,),
            ).fetchall():
                dst.execute(
                    "INSERT INTO change_log (item_id, field, old_value, new_value, changed_at) "
                    "VALUES (?,?,?,?,?)",
                    (id_map[cl["item_id"]], cl["field"], cl["old_value"],
                     cl["new_value"], cl["changed_at"]),
                )
            for rb in conn.execute(
                "SELECT * FROM review_batches WHERE collection_id=?", (coll_id,)
            ).fetchall():
                dst.execute(
                    "INSERT INTO review_batches (collection_id, lang_code, export_path, "
                    "exported_at, imported_at, item_count, status) VALUES (?,?,?,?,?,?,?)",
                    (coll_id, rb["lang_code"], rb["export_path"], rb["exported_at"],
                     rb["imported_at"], rb["item_count"], rb["status"]),
                )
            _fts_rebuild_conn(dst)
        finally:
            dst.close()

    for table in ("items", "item_collections", "change_log", "review_batches"):
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
    conn.execute("INSERT OR REPLACE INTO meta (key,value) VALUES ('per_coll_dbs','1')")
    conn.commit()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(COLL_DB_DIR, exist_ok=True)

    conn = get_db()
    try:
        conn.executescript(_CATALOG_SCHEMA_SQL)
        conn.commit()
        _migrate_legacy(conn)
        rows = conn.execute("SELECT id, identifier FROM collections").fetchall()
    finally:
        conn.close()

    for row in rows:
        _IDENT_CACHE[row["id"]] = row["identifier"]
        cconn = get_coll_db_conn(row["identifier"])
        cconn.close()
    cache_clear()


# ── Collections (catalog) ─────────────────────────────────────────────────────

def list_collections():
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM collections ORDER BY name").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_collection(coll_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM collections WHERE id=?", (coll_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def add_collection(identifier, name, description=""):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO collections (identifier, name, description) VALUES (?,?,?)",
            (identifier.strip(), name.strip(), description.strip()),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM collections WHERE identifier=?", (identifier,)
        ).fetchone()
        coll = dict(row)
        _IDENT_CACHE[coll["id"]] = identifier
        cconn = get_coll_db_conn(identifier)
        cconn.close()
        cache_clear()
        return coll
    finally:
        conn.close()


def delete_collection(coll_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT identifier FROM collections WHERE id=?", (coll_id,)
        ).fetchone()
        if not row:
            return
        ident = row[0]
    finally:
        conn.close()

    cancel_jobs_for_collection(coll_id)
    conn = get_db()
    try:
        conn.execute("DELETE FROM collections WHERE id=?", (coll_id,))
        conn.commit()
    finally:
        conn.close()

    _IDENT_CACHE.pop(coll_id, None)
    cache_clear()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(_coll_path(ident) + suffix)
        except OSError:
            pass


def update_collection_sync(coll_id, item_count):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE collections SET last_synced=?, item_count=? WHERE id=?",
            (datetime.utcnow().isoformat(), item_count, coll_id),
        )
        conn.commit()
    finally:
        conn.close()
    cache_clear()


# ── Sub-collection discovery (catalog) ────────────────────────────────────────

def upsert_sub_collection(super_id, ia_id, name):
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO ia_sub_collections (super_id, ia_id, name, discovered_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(super_id, ia_id) DO UPDATE
                SET name=excluded.name, discovered_at=excluded.discovered_at
        """, (super_id, ia_id, name or ia_id))
        conn.commit()
    finally:
        conn.close()
    cache_clear()


def clear_sub_collections(super_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM ia_sub_collections WHERE super_id=?", (super_id,))
        conn.commit()
    finally:
        conn.close()
    cache_clear()


def get_collections_tree():
    cached = cache_get(("tree",))
    if cached is not None:
        return cached

    conn = get_db()
    try:
        supers = conn.execute("SELECT * FROM collections ORDER BY name").fetchall()
        supers = [dict(s) for s in supers]
    finally:
        conn.close()

    tree = []
    for s in supers:
        stats = get_stats(s["id"])
        entry = dict(s)
        entry["local_count"] = stats["total"]
        conn = get_db()
        try:
            subs = conn.execute(
                "SELECT * FROM ia_sub_collections WHERE super_id=? ORDER BY name",
                (s["id"],),
            ).fetchall()
        finally:
            conn.close()
        sub_list = []
        for sub in subs:
            d = dict(sub)
            d["local_count"] = _sub_local_count(s["id"], sub["ia_id"])
            sub_list.append(d)
        entry["sub_collections"] = sub_list
        tree.append(entry)

    cache_put(("tree",), tree, 3)
    return tree


def _sub_local_count(coll_id, ia_coll):
    key = ("sub", coll_id, ia_coll)
    cached = cache_get(key)
    if cached is not None:
        return cached
    conn = get_coll_db(coll_id)
    try:
        n = conn.execute(
            """SELECT COUNT(DISTINCT ic.item_id) FROM item_collections ic
               JOIN items it ON it.id = ic.item_id WHERE ic.collection = ?""",
            (ia_coll,),
        ).fetchone()[0]
    finally:
        conn.close()
    cache_put(key, n, 4)
    return n


# ── Items (per-collection DB) ─────────────────────────────────────────────────

CORE_FIELDS = [
    "title", "alt_title", "creator", "alt_creator", "author", "alt_author",
    "publisher", "alt_publisher", "date", "year", "language", "subject",
    "description", "licenseurl", "mediatype", "volume", "isbn", "source", "notes"
]


def _fts_upsert(conn, item_id, metadata_dict):
    if not fts_supported():
        return
    try:
        conn.execute("DELETE FROM items_fts WHERE rowid=?", (item_id,))
        conn.execute(
            "INSERT INTO items_fts (rowid, identifier, title, creator, author, publisher, subject) "
            "VALUES (?,?,?,?,?,?,?)",
            (item_id, metadata_dict.get("identifier"), metadata_dict.get("title"),
             metadata_dict.get("creator"), metadata_dict.get("author"),
             metadata_dict.get("publisher"), metadata_dict.get("subject")),
        )
    except Exception:
        try:
            conn.execute("DELETE FROM items_fts WHERE rowid=?", (item_id,))
        except Exception:
            pass


def upsert_item(collection_id, identifier, metadata_dict, ia_raw=None):
    """Insert or update an item from IA metadata in the collection's DB."""
    import transliteration as T

    conn = get_coll_db(collection_id)
    try:
        existing = conn.execute(
            "SELECT id, detected_language, translit_status FROM items WHERE identifier=?",
            (identifier,),
        ).fetchone()

        core = {f: metadata_dict.get(f) for f in CORE_FIELDS}
        core["downloads"] = metadata_dict.get("downloads") or 0
        extra = {k: v for k, v in metadata_dict.items()
                 if k not in CORE_FIELDS and k not in ('identifier', '_collections')}
        core["extra_metadata"] = json.dumps(extra) if extra else None
        core["ia_raw"]         = json.dumps(ia_raw) if ia_raw else None
        core["last_synced"]    = datetime.utcnow().isoformat()
        core["ia_updatedate"]  = metadata_dict.get("updatedate") or metadata_dict.get("publicdate")

        raw_lang = metadata_dict.get("language") or ""
        detected = T.normalize_language(raw_lang) or T.normalize_language(identifier.split("-")[-1])
        core["detected_language"] = detected

        if existing:
            item_id = existing["id"]
            if existing["translit_status"] not in (None, "none"):
                core.pop("translit_status", None)
            sets = ", ".join(f"{k}=?" for k in core)
            conn.execute(f"UPDATE items SET {sets} WHERE id=?", list(core.values()) + [item_id])
        else:
            core["translit_status"] = "none"
            local_seq = conn.execute(
                "SELECT COALESCE(MAX(id), 0) % ? + 1 FROM items", (ID_OFFSET,)
            ).fetchone()[0]
            item_id = collection_id * ID_OFFSET + local_seq
            cols = ["id", "identifier"] + list(core.keys())
            conn.execute(
                f"INSERT INTO items ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                [item_id, identifier] + list(core.values()),
            )

        colls = metadata_dict.get("_collections", [])
        if colls:
            conn.execute("DELETE FROM item_collections WHERE item_id=?", (item_id,))
            conn.executemany(
                "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                [(item_id, cl) for cl in colls],
            )

        _fts_upsert(conn, item_id, metadata_dict)
        conn.commit()
        _dirty_coll(collection_id)
        return item_id
    finally:
        conn.close()


def batch_upsert_items(collection_id, items, ia_raw=True):
    """Upsert a list of IA item metadata dicts using a single DB connection.
    Much faster than calling upsert_item() per item for large syncs."""
    import transliteration as T

    items = [it for it in items if it.get("identifier")]
    if not items:
        return 0

    conn = get_coll_db(collection_id)
    try:
        idents = list(dict.fromkeys(it["identifier"] for it in items))
        marks = ",".join("?" * len(idents))
        rows = conn.execute(
            f"SELECT identifier, id, translit_status FROM items "
            f"WHERE identifier IN ({marks})", idents,
        ).fetchall()
        existing = {r["identifier"]: dict(r) for r in rows}

        max_row = conn.execute("SELECT MAX(id) FROM items").fetchone()[0]
        local_seq = (max_row % ID_OFFSET) if max_row else 0

        for item in items:
            identifier = item["identifier"]
            core = {f: item.get(f) for f in CORE_FIELDS}
            core["downloads"] = item.get("downloads") or 0
            extra = {k: v for k, v in item.items()
                     if k not in CORE_FIELDS and k not in ('identifier', '_collections')}
            core["extra_metadata"] = json.dumps(extra) if extra else None
            core["ia_raw"]         = json.dumps(item) if ia_raw else None
            core["last_synced"]    = datetime.utcnow().isoformat()
            core["ia_updatedate"]  = item.get("updatedate") or item.get("publicdate")

            raw_lang = item.get("language") or ""
            detected = T.normalize_language(raw_lang) or T.normalize_language(identifier.split("-")[-1])
            core["detected_language"] = detected

            ex = existing.get(identifier)
            if ex:
                item_id = ex["id"]
                if ex.get("translit_status") not in (None, "none"):
                    core.pop("translit_status", None)
                sets = ", ".join(f"{k}=?" for k in core)
                conn.execute(f"UPDATE items SET {sets} WHERE id=?",
                             list(core.values()) + [item_id])
            else:
                core["translit_status"] = "none"
                local_seq += 1
                item_id = collection_id * ID_OFFSET + local_seq
                cols = ["id", "identifier"] + list(core.keys())
                conn.execute(
                    f"INSERT INTO items ({', '.join(cols)}) "
                    f"VALUES ({', '.join('?' * len(cols))})",
                    [item_id, identifier] + list(core.values()))

            colls = item.get("_collections", [])
            if colls:
                conn.execute("DELETE FROM item_collections WHERE item_id=?", (item_id,))
                conn.executemany(
                    "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                    [(item_id, cl) for cl in colls],
                )
            _fts_upsert(conn, item_id, item)
            existing[identifier] = {"id": item_id,
                                    "translit_status": core.get("translit_status")}

        conn.commit()
        _dirty_coll(collection_id)
        return len(items)
    finally:
        conn.close()


def collection_prefix_buckets(collection_id, n_buckets):
    """Return a list of (prefix, count) identifier prefixes that partition the
    collection's local items into ~``n_buckets`` roughly equal-sized groups.
    Prefixes are prefix-free (no prefix is a prefix of another) and every item
    matches exactly one, so ``identifier:{prefix}*`` queries partition the
    whole collection.  Used to run independent IA scrape cursor chains in
    parallel (one per prefix).

    Returns [] when the collection is empty or too small to split.
    """
    from collections import deque

    conn = get_coll_db(collection_id)
    try:
        total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        if total < n_buckets * 50:
            return []
        target = -(-total // n_buckets)  # ceil
        buckets, queue = [], deque([("", total)])
        while queue:
            prefix, cnt = queue.popleft()
            if cnt <= target:
                if prefix:
                    buckets.append((prefix, cnt))
                continue
            L = len(prefix)
            rows = conn.execute(
                "SELECT substr(identifier,1,?) AS p, COUNT(*) AS c FROM items "
                "WHERE identifier >= ? AND identifier < ? || char(0x10FFFF) "
                "GROUP BY p",
                (L + 1, prefix, prefix),
            ).fetchall()
            for r in rows:
                queue.append((r["p"], r["c"]))
        return buckets
    finally:
        conn.close()


def _build_selection_sql(selection):
    """Build (where_clauses, params) for a bulk selection dict against a collection DB."""
    where, params = [], []
    if selection.get("match_all"):
        return where, params
    ids = selection.get("ids")
    if isinstance(ids, (list, tuple)) and ids:
        ids = [int(x) for x in ids]
        where.append("i.id IN (" + ",".join("?" * len(ids)) + ")")
        params.extend(ids)
        return where, params
    match_field = (selection.get("match_field") or "").strip()
    if match_field and selection.get("match_pattern"):
        pattern = selection["match_pattern"]
        if selection.get("exact"):
            where.append(f"i.{match_field} = ?")
            params.append(pattern)
        else:
            where.append(f"i.{match_field} LIKE ?")
            params.append(f"%{pattern}%")
    else:
        q = (selection.get("q") or "").strip()
        if q:
            where.append("(i.title LIKE ? OR i.identifier LIKE ? OR i.creator LIKE ? "
                         "OR i.author LIKE ? OR i.publisher LIKE ? OR i.subject LIKE ?)")
            s = f"%{q}%"
            params.extend([s] * 6)
        if selection.get("ia_collection"):
            where.append("i.id IN (SELECT item_id FROM item_collections WHERE collection = ? COLLATE NOCASE)")
            params.append(selection["ia_collection"])
        if selection.get("ia_collection_not"):
            where.append("i.id NOT IN (SELECT item_id FROM item_collections WHERE collection = ? COLLATE NOCASE)")
            params.append(selection["ia_collection_not"])
        if selection.get("modified_only"):
            where.append("i.is_modified = 1")
        if selection.get("lang"):
            where.append("i.detected_language = ?")
            params.append(selection["lang"])
        if selection.get("tstatus"):
            where.append("i.translit_status = ?")
            params.append(selection["tstatus"])
    return where, params


def count_items(collection_id, selection=None):
    selection = selection or {}
    conn = get_coll_db(collection_id)
    try:
        where, params = _build_selection_sql(selection)
        sql = "SELECT COUNT(*) FROM items i WHERE " + (" AND ".join(where) if where else "1=1")
        return conn.execute(sql, params).fetchone()[0]
    finally:
        conn.close()


def select_items(collection_id, selection=None):
    """Return matching items (id, identifier, membership list) for bulk operations."""
    selection = selection or {}
    conn = get_coll_db(collection_id)
    try:
        where, params = _build_selection_sql(selection)
        sql = ("SELECT i.id, i.identifier, GROUP_CONCAT(ic.collection, '||') AS collections "
               "FROM items i LEFT JOIN item_collections ic ON ic.item_id = i.id WHERE "
               + (" AND ".join(where) if where else "1=1")
               + " GROUP BY i.id ORDER BY i.identifier")
        out = []
        for r in conn.execute(sql, params).fetchall():
            d = dict(r)
            d["collections"] = (d.get("collections") or "").split("||") \
                if d.get("collections") else []
            out.append(d)
        return out
    finally:
        conn.close()


def set_item_membership(collection_id, item_id, coll_name, present):
    conn = get_coll_db(collection_id)
    try:
        if present:
            conn.execute(
                "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                (item_id, coll_name),
            )
        else:
            conn.execute(
                "DELETE FROM item_collections WHERE item_id=? AND collection=?",
                (item_id, coll_name),
            )
        conn.commit()
        _dirty_coll(collection_id)
    finally:
        conn.close()


def list_items(collection_id, search=None, modified_only=False,
               lang_code=None, translit_status=None,
               ia_collection=None, ia_collection_not=None,
               page=1, per_page=50, sort="title", sort_dir="asc"):
    conn = get_coll_db(collection_id)
    where, params = [], []

    if ia_collection:
        where.append("i.id IN (SELECT item_id FROM item_collections WHERE collection = ? COLLATE NOCASE)")
        params.append(ia_collection)
    if ia_collection_not:
        where.append("i.id NOT IN (SELECT item_id FROM item_collections WHERE collection = ? COLLATE NOCASE)")
        params.append(ia_collection_not)
    if modified_only:
        where.append("i.is_modified=1")
    if lang_code:
        where.append("i.detected_language=?")
        params.append(lang_code)
    if translit_status:
        where.append("i.translit_status=?")
        params.append(translit_status)
    if search:
        fq = _fts_query(search)
        if fts_supported() and fq:
            where.append("i.id IN (SELECT rowid FROM items_fts WHERE items_fts MATCH ?)")
            params.append(fq)
        else:
            where.append(
                "(i.title LIKE ? OR i.identifier LIKE ? OR i.creator LIKE ? "
                "OR i.author LIKE ? OR i.publisher LIKE ? OR i.subject LIKE ?)"
            )
            s = f"%{search}%"
            params.extend([s, s, s, s, s, s])

    where_sql = " AND ".join(where) if where else "1=1"

    allowed = {"title", "identifier", "creator", "author", "publisher",
               "date", "year", "last_modified", "last_synced", "detected_language"}
    if sort not in allowed:
        sort = "title"
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
    offset = (page - 1) * per_page

    total = conn.execute(
        f"SELECT COUNT(*) FROM items i WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT i.*, GROUP_CONCAT(ic.collection,'||') as collections
            FROM items i
            LEFT JOIN item_collections ic ON ic.item_id=i.id
            WHERE {where_sql}
            GROUP BY i.id
            ORDER BY i.{sort} {direction} NULLS LAST
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    conn.close()
    return {"total": total, "page": page, "per_page": per_page,
            "items": [dict(r) for r in rows]}


def get_item(item_id):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        row = conn.execute(
            """SELECT i.*, GROUP_CONCAT(ic.collection,'||') as collections
               FROM items i
               LEFT JOIN item_collections ic ON ic.item_id=i.id
               WHERE i.id=? GROUP BY i.id""",
            (item_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    item = dict(row)
    item["collection_id"] = coll_id
    if item.get("extra_metadata"):
        try:
            item["extra_metadata"] = json.loads(item["extra_metadata"])
        except Exception:
            pass
    if item.get("ia_raw"):
        try:
            item["ia_raw"] = json.loads(item["ia_raw"])
        except Exception:
            pass
    return item


def update_item_fields(item_id, fields: dict):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        current = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if not current:
            return False

        updatable = set(CORE_FIELDS) | {"translit_status", "detected_language", "extra_metadata"}
        sets, vals, log_entries = [], [], []

        for field, new_val in fields.items():
            if field in updatable:
                old_val = current[field] if field in current.keys() else None
                if str(old_val or "") != str(new_val or ""):
                    sets.append(f"{field}=?")
                    vals.append(new_val)
                    if field in CORE_FIELDS:
                        log_entries.append((item_id, field, old_val, new_val))

        if sets:
            real_change = any(f in CORE_FIELDS for f in fields)
            if real_change:
                sets += ["is_modified=1", "is_pushed=0", "last_modified=?"]
                vals.append(datetime.utcnow().isoformat())
            vals.append(item_id)
            conn.execute(f"UPDATE items SET {', '.join(sets)} WHERE id=?", vals)
            if log_entries:
                conn.executemany(
                    "INSERT INTO change_log (item_id,field,old_value,new_value) VALUES (?,?,?,?)",
                    log_entries,
                )

        if "collections" in fields:
            colls = [c.strip() for c in fields["collections"].split("||") if c.strip()]
            conn.execute("DELETE FROM item_collections WHERE item_id=?", (item_id,))
            for cl in colls:
                conn.execute(
                    "INSERT OR IGNORE INTO item_collections (item_id,collection) VALUES (?,?)",
                    (item_id, cl),
                )

        conn.commit()
        _dirty_coll(coll_id)
        return True
    finally:
        conn.close()


def bulk_update(collection_id, match_field, match_pattern,
                update_fields: dict, exact=False):
    conn = get_coll_db(collection_id)
    try:
        if exact:
            rows = conn.execute(
                f"SELECT id FROM items WHERE {match_field}=?", (match_pattern,)
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT id FROM items WHERE {match_field} LIKE ?",
                (f"%{match_pattern}%",),
            ).fetchall()
        updated = 0
        for row in rows:
            if update_item_fields(row["id"], update_fields):
                updated += 1
        _dirty_coll(collection_id)
        return updated
    finally:
        conn.close()


def bulk_update_by_ids(collection_id, ids, fields: dict):
    """Apply field updates to an explicit list of item ids (checkbox selection)."""
    conn = get_coll_db(collection_id)
    try:
        updated = 0
        for row in conn.execute(
            "SELECT id FROM items WHERE id IN (" + ",".join("?" * len(ids)) + ")",
            list(ids),
        ).fetchall():
            if update_item_fields(row["id"], fields):
                updated += 1
        _dirty_coll(collection_id)
        return updated
    finally:
        conn.close()


def get_collection_names(collection_id, min_count=2, limit=500):
    """List the IA collections present in this collection's items, with counts,
    ordered by population. Used to answer 'which collections are the items
    available in'. Small/one-off collections (unique per-item collections) are
    excluded via min_count so the list stays useful."""
    key = ("collnames", collection_id, min_count, limit)
    cached = cache_get(key)
    if cached is not None:
        return cached
    conn = get_coll_db(collection_id)
    try:
        rows = conn.execute(
            "SELECT collection AS name, COUNT(DISTINCT item_id) AS count "
            "FROM item_collections "
            "GROUP BY collection HAVING COUNT(DISTINCT item_id)>=? "
            "ORDER BY count DESC, name LIMIT ?",
            (min_count, limit),
        ).fetchall()
        out = [dict(r) for r in rows]
    finally:
        conn.close()
    cache_put(key, out, 4)
    return out


def get_item_changes(item_id):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        rows = conn.execute(
            "SELECT * FROM change_log WHERE item_id=? ORDER BY changed_at DESC",
            (item_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_pushed(item_id):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        conn.execute(
            "UPDATE items SET is_pushed=1, last_pushed=? WHERE id=?",
            (datetime.utcnow().isoformat(), item_id),
        )
        conn.commit()
        _dirty_coll(coll_id)
    finally:
        conn.close()


def get_item_identifiers(collection_id, after_id=None, limit=100):
    """Return item id/identifier rows for a collection, ordered by id.
    Pass ``after_id`` to page through large collections (resumable stats sync)."""
    conn = get_coll_db(collection_id)
    try:
        if after_id:
            rows = conn.execute(
                "SELECT id, identifier FROM items WHERE id>? ORDER BY id LIMIT ?",
                (after_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, identifier FROM items ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_item_stats_batch(collection_id, stats_map):
    """Write IA view/download stats keyed by identifier in one connection."""
    conn = get_coll_db(collection_id)
    try:
        conn.executemany(
            "UPDATE items SET downloads=?, views_30d=?, views_7d=? WHERE identifier=?",
            [(s.get("all_time") or 0, s.get("last_30day") or 0,
              s.get("last_7day") or 0, ident)
             for ident, s in stats_map.items()],
        )
        conn.commit()
        _dirty_coll(collection_id)
    finally:
        conn.close()


def get_modified_items(collection_id):
    conn = get_coll_db(collection_id)
    try:
        rows = conn.execute(
            """SELECT i.*, GROUP_CONCAT(ic.collection,'||') as collections
               FROM items i
               LEFT JOIN item_collections ic ON ic.item_id=i.id
               WHERE i.is_modified=1
               GROUP BY i.id ORDER BY i.title""",
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_stats(collection_id, ia_collection=None):
    key = ("stats", collection_id, ia_collection)
    cached = cache_get(key)
    if cached is not None:
        return cached
    conn = get_coll_db(collection_id)
    try:
        if ia_collection:
            sub_where = ("i.id IN (SELECT item_id FROM item_collections "
                         "WHERE collection=?)")
            total    = conn.execute(
                f"SELECT COUNT(*) FROM items i WHERE {sub_where}",
                (ia_collection,)).fetchone()[0]
            modified = conn.execute(
                f"SELECT COUNT(*) FROM items i WHERE i.is_modified=1 AND {sub_where}",
                (ia_collection,)).fetchone()[0]
            pushed   = conn.execute(
                f"SELECT COUNT(*) FROM items i WHERE i.is_pushed=1 AND {sub_where}",
                (ia_collection,)).fetchone()[0]
        else:
            total    = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
            modified = conn.execute(
                "SELECT COUNT(*) FROM items WHERE is_modified=1").fetchone()[0]
            pushed   = conn.execute(
                "SELECT COUNT(*) FROM items WHERE is_pushed=1").fetchone()[0]
    finally:
        conn.close()
    out = {"total": total, "modified": modified, "pushed": pushed,
           "pending_push": modified - pushed}
    cache_put(key, out, 3)
    return out


# ── Language & transliteration (per-collection DB) ───────────────────────────

def get_language_breakdown(collection_id, ia_collection=None, modified_only=False):
    key = ("lang", collection_id, ia_collection or "", bool(modified_only))
    cached = cache_get(key)
    if cached is not None:
        return cached
    conn = get_coll_db(collection_id)
    try:
        where, params = [], []
        if ia_collection:
            where.append(
                "id IN (SELECT item_id FROM item_collections WHERE collection=?)"
            )
            params.append(ia_collection)
        if modified_only:
            where.append("is_modified=1")
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            f"""SELECT detected_language, translit_status, COUNT(*) as cnt
                FROM items{where_sql}
                GROUP BY detected_language, translit_status""",
            params,
        ).fetchall()
    finally:
        conn.close()

    langs: dict = {}
    for r in rows:
        code   = r["detected_language"] or "unknown"
        status = r["translit_status"]   or "none"
        cnt    = r["cnt"]
        if code not in langs:
            langs[code] = {"code": code, "total": 0,
                           "none": 0, "copied": 0, "generated": 0,
                           "reviewed": 0, "finalized": 0}
        langs[code]["total"] += cnt
        langs[code][status if status in langs[code] else "none"] += cnt
    out = list(langs.values())
    cache_put(key, out, 4)
    return out


def get_items_for_transliteration(collection_id, lang_code, status_filter=None):
    conn = get_coll_db(collection_id)
    where = ["detected_language=?"]
    params = [lang_code]
    if status_filter:
        where.append("translit_status=?")
        params.append(status_filter)
    try:
        rows = conn.execute(
            f"SELECT * FROM items WHERE {' AND '.join(where)} ORDER BY title",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_ia_updatedate_max(collection_id):
    conn = get_coll_db(collection_id)
    try:
        row = conn.execute("SELECT MAX(ia_updatedate) FROM items").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


# ── Review batches (per-collection DB) ────────────────────────────────────────

def create_review_batch(collection_id, lang_code, export_path, item_count):
    conn = get_coll_db(collection_id)
    try:
        conn.execute(
            """INSERT INTO review_batches
               (collection_id, lang_code, export_path, exported_at, item_count, status)
               VALUES (?,?,?,?,?,'exported')""",
            (collection_id, lang_code, export_path,
             datetime.utcnow().isoformat(), item_count),
        )
        conn.commit()
    finally:
        conn.close()


def list_review_batches(collection_id):
    conn = get_coll_db(collection_id)
    try:
        rows = conn.execute(
            "SELECT * FROM review_batches ORDER BY exported_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Revert / undo (per-collection DB) ─────────────────────────────────────────

def revert_item_to_ia(item_id):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if not row:
            return None
        item = dict(row)
        ia_raw_str = item.get("ia_raw")
        if not ia_raw_str:
            return None
        try:
            ia_raw = json.loads(ia_raw_str)
        except Exception:
            return None

        revert_vals = {}
        log_entries = []
        for field in CORE_FIELDS:
            ia_val  = ia_raw.get(field)
            cur_val = item.get(field)
            if str(ia_val or "") != str(cur_val or ""):
                revert_vals[field] = ia_val
                log_entries.append((item_id, field, cur_val, ia_val))

        if revert_vals:
            sets = ", ".join(f"{k}=?" for k in revert_vals)
            vals = list(revert_vals.values())
            conn.execute(
                f"UPDATE items SET {sets}, is_modified=0, is_pushed=0, last_modified=? "
                "WHERE id=?",
                vals + [datetime.utcnow().isoformat(), item_id],
            )
            conn.executemany(
                "INSERT INTO change_log (item_id,field,old_value,new_value) VALUES (?,?,?,?)",
                log_entries,
            )
        else:
            conn.execute(
                "UPDATE items SET is_modified=0, is_pushed=0 WHERE id=?", (item_id,)
            )
        conn.commit()
        _dirty_coll(coll_id)
        return dict(conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone())
    finally:
        conn.close()


def revert_item_field(item_id, change_id):
    coll_id = _coll_id_from_item_id(item_id)
    conn = get_coll_db(coll_id)
    try:
        change = conn.execute(
            "SELECT * FROM change_log WHERE id=? AND item_id=?",
            (change_id, item_id),
        ).fetchone()
        if not change:
            return None
        field     = change["field"]
        old_value = change["old_value"]
        cur_row   = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if not cur_row:
            return None
        cur_val = cur_row[field] if field in cur_row.keys() else None
        conn.execute(
            f"UPDATE items SET {field}=?, is_modified=1, last_modified=? WHERE id=?",
            (old_value, datetime.utcnow().isoformat(), item_id),
        )
        conn.execute(
            "INSERT INTO change_log (item_id,field,old_value,new_value) VALUES (?,?,?,?)",
            (item_id, field, cur_val, old_value),
        )
        conn.commit()
        _dirty_coll(coll_id)
        return {"field": field, "reverted_to": old_value}
    finally:
        conn.close()


# ── Index & DB maintenance ────────────────────────────────────────────────────

def fts_rebuild_coll(collection_id):
    conn = get_coll_db(collection_id)
    try:
        n = _fts_rebuild_conn(conn)
        _dirty_coll(collection_id)
        return n
    finally:
        conn.close()


def fts_status(collection_id):
    conn = get_coll_db(collection_id)
    try:
        items = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        if not fts_supported():
            return {"items": items, "indexed": None}
        try:
            indexed = conn.execute("SELECT COUNT(*) FROM items_fts").fetchone()[0]
        except Exception:
            indexed = 0
        return {"items": items, "indexed": indexed}
    finally:
        conn.close()


def vacuum_coll(collection_id):
    conn = get_coll_db(collection_id)
    try:
        conn.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()


def collection_db_info(collection_id):
    ident = _coll_identifier(collection_id)
    path  = _coll_path(ident)
    size  = os.path.getsize(path) if os.path.exists(path) else 0
    st    = get_stats(collection_id)
    fs    = fts_status(collection_id)
    conn  = get_db()
    try:
        row = conn.execute(
            "SELECT name, identifier, last_synced FROM collections WHERE id=?",
            (collection_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"id": collection_id, "name": str(collection_id), "identifier": ident,
                "db_file": path, "size_kb": round(size / 1024, 1),
                "total": st["total"], "modified": st["modified"], "pushed": st["pushed"],
                "last_synced": None, "fts": fs}
    return {"id": collection_id, "name": row["name"], "identifier": row["identifier"],
            "db_file": path, "size_kb": round(size / 1024, 1),
            "total": st["total"], "modified": st["modified"], "pushed": st["pushed"],
            "last_synced": row["last_synced"], "fts": fs}


# ── Background job queue (catalog) ────────────────────────────────────────────

def create_job(job_type, collection_id=None, params=None, title=""):
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, collection_id, title, params, status, created_at) "
            "VALUES (?,?,?,?,'queued', datetime('now'))",
            (job_type, collection_id, title or job_type, json.dumps(params or {})),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def claim_next_job():
    """Claim the oldest queued job whose collection has no job already running.
    This lets multiple worker threads sync DIFFERENT collections in parallel
    while guaranteeing a collection is never synced by two workers at once."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='queued' AND cancel_requested=0 "
            "AND collection_id NOT IN ("
            "  SELECT collection_id FROM jobs WHERE status='running') "
            "ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE jobs SET status='running', started_at=datetime('now') WHERE id=?",
            (row["id"],),
        )
        conn.commit()
        return dict(row)
    finally:
        conn.close()


def set_job_resume(job_id, key, value):
    """Persist a resume marker (scrape cursor / page number) on a job's params
    so an interrupted sync can continue where it left off after a restart."""
    conn = get_db()
    try:
        row = conn.execute("SELECT params FROM jobs WHERE id=?", (job_id,)).fetchone()
        params = {}
        if row and row["params"]:
            try:
                params = json.loads(row["params"])
            except Exception:
                params = {}
        params[key] = value
        conn.execute("UPDATE jobs SET params=? WHERE id=?", (json.dumps(params), job_id))
        conn.commit()
    finally:
        conn.close()


def latest_sync_resume(coll_id, job_type, include_noindex=None):
    """Return resume markers from the most recent failed/interrupted sync of the
    same type, so a freshly queued sync can continue instead of repeating work
    already done.  Returns {'current','resume_cursor','resume_page'} or None."""
    conn = get_db()
    try:
        if include_noindex is None:
            rows = conn.execute(
                "SELECT params, current FROM jobs "
                "WHERE collection_id=? AND job_type=? AND status IN ('error','interrupted','cancelled') "
                "ORDER BY id DESC LIMIT 20",
                (coll_id, job_type),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT params, current FROM jobs "
                "WHERE collection_id=? AND job_type=? AND status IN ('error','interrupted','cancelled') "
                "AND json_extract(params,'$.include_noindex')=? "
                "ORDER BY id DESC LIMIT 20",
                (coll_id, job_type, 1 if include_noindex else 0),
            ).fetchall()
        for row in rows:
            params = {}
            if row["params"]:
                try:
                    params = json.loads(row["params"])
                except Exception:
                    params = {}
            out = {"current": int(row["current"] or 0)}
            if params.get("resume_cursor"):
                out["resume_cursor"] = params["resume_cursor"]
            if params.get("resume_page"):
                out["resume_page"] = params["resume_page"]
            if "resume_cursor" in out or "resume_page" in out:
                return out
        return None
    finally:
        conn.close()


def update_job_progress(job_id, current=None, total=None):
    conn = get_db()
    try:
        if current is not None and total is not None:
            conn.execute("UPDATE jobs SET current=?, total=? WHERE id=?", (current, total, job_id))
        else:
            sets, vals = [], []
            if current is not None:
                sets.append("current=?"); vals.append(current)
            if total is not None:
                sets.append("total=?"); vals.append(total)
            if sets:
                conn.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id=?", vals + [job_id])
        conn.commit()
    finally:
        conn.close()


def finish_job(job_id, status="done", new_count=None, error=None):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE jobs SET status=?, new_count=COALESCE(?, new_count), error=?, "
            "finished_at=datetime('now') WHERE id=?",
            (status, new_count, error, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def fail_job(job_id, error):
    finish_job(job_id, "error", error=str(error))


def add_job_log(job_id, level, message):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO job_log (job_id, level, message) VALUES (?,?,?)",
            (job_id, level, str(message)),
        )
        conn.commit()
    finally:
        conn.close()


def cancel_job(job_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return
        if row["status"] == "queued":
            conn.execute(
                "UPDATE jobs SET status='cancelled', finished_at=datetime('now') WHERE id=?",
                (job_id,),
            )
        elif row["status"] == "running":
            conn.execute("UPDATE jobs SET cancel_requested=1 WHERE id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()


def job_cancel_requested(job_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
        return bool(row and row[0])
    finally:
        conn.close()


def get_job(job_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        job = dict(row) if row else None
        if job:
            try:
                job["params"] = json.loads(job["params"] or "{}")
            except Exception:
                job["params"] = {}
        return job
    finally:
        conn.close()


def list_jobs(collection_id=None, limit=100):
    conn = get_db()
    try:
        if collection_id:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE collection_id=? ORDER BY id DESC LIMIT ?",
                (collection_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for r in rows:
            j = dict(r)
            try:
                j["params"] = json.loads(j["params"] or "{}")
            except Exception:
                j["params"] = {}
            out.append(j)
        return out
    finally:
        conn.close()


def job_counts():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM jobs GROUP BY status"
        ).fetchall()
        return {r["status"]: r["c"] for r in rows}
    finally:
        conn.close()


def get_job_log(job_id, limit=500):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM job_log WHERE job_id=? ORDER BY id ASC LIMIT ?",
            (job_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def latest_job_for_collection(collection_id, types=None):
    conn = get_db()
    try:
        if types:
            ph = ",".join("?" * len(types))
            row = conn.execute(
                f"SELECT * FROM jobs WHERE collection_id=? AND job_type IN ({ph}) "
                "ORDER BY id DESC LIMIT 1",
                (collection_id, *types),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM jobs WHERE collection_id=? ORDER BY id DESC LIMIT 1",
                (collection_id,),
            ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def mark_interrupted_jobs():
    conn = get_db()
    try:
        conn.execute(
            "UPDATE jobs SET status='interrupted', "
            "error=COALESCE(error, 'interrupted by restart'), "
            "finished_at=datetime('now') WHERE status='running'"
        )
        conn.commit()
        # Resumable syncs continue from their saved cursor after a restart
        # instead of repeating work already done.  Other job types stay
        # 'interrupted' (re-run them manually).  Jobs the user cancelled
        # (cancel_requested=1) are never resumed.  For each (collection, sync
        # type) only ONE job is re-queued: the newest that has a saved resume
        # marker if any exists, otherwise the newest overall — so a restart
        # never leaves multiple competing syncs running for one collection.
        rows = conn.execute(
            "SELECT id, collection_id, job_type, params FROM jobs "
            "WHERE status='interrupted' AND cancel_requested=0 "
            "AND job_type IN ('full-sync','smart-sync') "
            "ORDER BY id"
        ).fetchall()

        def has_resume(params_json):
            try:
                p = json.loads(params_json or "{}")
            except Exception:
                p = {}
            return bool(p.get("resume_cursor") or p.get("resume_page"))

        best = {}  # (coll_id, job_type) -> [id, has_resume_marker]
        for r in rows:
            key = (r["collection_id"], r["job_type"])
            seen = best.get(key)
            if seen is None:
                best[key] = [r["id"], has_resume(r["params"])]
                continue
            cur_has = seen[1]
            new_has = has_resume(r["params"])
            if new_has and not cur_has:
                best[key] = [r["id"], True]
            elif new_has == cur_has and r["id"] > seen[0]:
                best[key] = [r["id"], new_has]
        resume_ids = [b[0] for b in best.values()]
        if resume_ids:
            conn.executemany(
                "UPDATE jobs SET status='queued', started_at=NULL, finished_at=NULL, "
                "error=NULL, cancel_requested=0 WHERE id=?",
                [(i,) for i in resume_ids],
            )
        # Cancel any other sync still queued for the same (collection, type) —
        # including ones that were already queued before the restart — so a
        # restart never leaves two competing syncs pending for one collection.
        for (coll_id, job_type), chosen in best.items():
            conn.execute(
                "UPDATE jobs SET status='cancelled', finished_at=datetime('now') "
                "WHERE status='queued' AND collection_id=? AND job_type=? AND id<>?",
                (coll_id, job_type, chosen[0]),
            )
        conn.commit()
    finally:
        conn.close()


def cancel_queued_syncs(collection_id, job_type=None):
    """Cancel queued sync/other jobs for a collection so a freshly queued job
    does not compete with an older pending one of the same type."""
    conn = get_db()
    try:
        if job_type:
            conn.execute(
                "UPDATE jobs SET status='cancelled', finished_at=datetime('now') "
                "WHERE status='queued' AND collection_id=? AND job_type=?",
                (collection_id, job_type),
            )
        else:
            conn.execute(
                "UPDATE jobs SET status='cancelled', finished_at=datetime('now') "
                "WHERE status='queued' AND collection_id=?",
                (collection_id,),
            )
        conn.commit()
    finally:
        conn.close()


def cancel_jobs_for_collection(collection_id):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE jobs SET status='cancelled', finished_at=datetime('now') "
            "WHERE collection_id=? AND status IN ('queued','running')",
            (collection_id,),
        )
        conn.commit()
    finally:
        conn.close()