"""
SOK IA Metadata Manager — Database Layer
SQLite-backed storage for collections, items, and metadata.
"""
import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "sok_metadata.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS collections (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        identifier  TEXT    UNIQUE NOT NULL,
        name        TEXT    NOT NULL,
        description TEXT,
        item_count  INTEGER DEFAULT 0,
        last_synced TEXT,
        created_at  TEXT    DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS items (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id   INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
        identifier      TEXT    NOT NULL,
        title           TEXT,
        alt_title       TEXT,
        creator         TEXT,
        alt_creator     TEXT,
        author          TEXT,
        alt_author      TEXT,
        publisher       TEXT,
        alt_publisher   TEXT,
        date            TEXT,
        year            TEXT,
        language        TEXT,
        subject         TEXT,
        description     TEXT,
        licenseurl      TEXT,
        mediatype       TEXT,
        volume          TEXT,
        isbn            TEXT,
        source          TEXT,
        notes           TEXT,
        extra_metadata  TEXT,   -- JSON blob for any extra IA fields
        ia_raw          TEXT,   -- full raw IA metadata JSON snapshot
        is_modified     INTEGER DEFAULT 0,
        is_pushed       INTEGER DEFAULT 0,
        last_synced     TEXT,
        last_modified   TEXT,
        last_pushed     TEXT,
        UNIQUE(collection_id, identifier)
    );

    CREATE TABLE IF NOT EXISTS item_collections (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id     INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
        collection  TEXT    NOT NULL,
        UNIQUE(item_id, collection)
    );

    CREATE TABLE IF NOT EXISTS change_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id     INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
        field       TEXT    NOT NULL,
        old_value   TEXT,
        new_value   TEXT,
        changed_at  TEXT    DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_items_collection  ON items(collection_id);
    CREATE INDEX IF NOT EXISTS idx_items_identifier  ON items(identifier);
    CREATE INDEX IF NOT EXISTS idx_items_modified    ON items(is_modified);
    CREATE INDEX IF NOT EXISTS idx_change_item       ON change_log(item_id);
    """)

    conn.commit()
    conn.close()


# ── Collections ──────────────────────────────────────────────────────────────

def list_collections():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM collections ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_collection(coll_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM collections WHERE id=?", (coll_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_collection(identifier, name, description=""):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO collections (identifier, name, description) VALUES (?,?,?)",
            (identifier.strip(), name.strip(), description.strip())
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM collections WHERE identifier=?", (identifier,)
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def delete_collection(coll_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM collections WHERE id=?", (coll_id,))
        conn.commit()
    finally:
        conn.close()


def update_collection_sync(coll_id, item_count):
    conn = get_db()
    conn.execute(
        "UPDATE collections SET last_synced=?, item_count=? WHERE id=?",
        (datetime.utcnow().isoformat(), item_count, coll_id)
    )
    conn.commit()
    conn.close()


# ── Items ────────────────────────────────────────────────────────────────────

CORE_FIELDS = [
    "title", "alt_title", "creator", "alt_creator", "author", "alt_author",
    "publisher", "alt_publisher", "date", "year", "language", "subject",
    "description", "licenseurl", "mediatype", "volume", "isbn", "source", "notes"
]


def upsert_item(collection_id, identifier, metadata_dict, ia_raw=None):
    """Insert or update an item. metadata_dict keys map to column names."""
    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT id FROM items WHERE collection_id=? AND identifier=?",
            (collection_id, identifier)
        ).fetchone()

        core = {f: metadata_dict.get(f) for f in CORE_FIELDS}
        extra = {k: v for k, v in metadata_dict.items()
                 if k not in CORE_FIELDS and k != "identifier"}
        core["extra_metadata"] = json.dumps(extra) if extra else None
        core["ia_raw"] = json.dumps(ia_raw) if ia_raw else None
        core["last_synced"] = datetime.utcnow().isoformat()

        if existing:
            item_id = existing["id"]
            sets = ", ".join(f"{k}=?" for k in core)
            vals = list(core.values()) + [item_id]
            conn.execute(f"UPDATE items SET {sets} WHERE id=?", vals)
        else:
            cols = ["collection_id", "identifier"] + list(core.keys())
            placeholders = ", ".join(["?"] * len(cols))
            vals = [collection_id, identifier] + list(core.values())
            conn.execute(
                f"INSERT INTO items ({', '.join(cols)}) VALUES ({placeholders})",
                vals
            )
            item_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Sync item_collections
        colls = metadata_dict.get("_collections", [])
        if colls:
            conn.execute("DELETE FROM item_collections WHERE item_id=?", (item_id,))
            for c in colls:
                conn.execute(
                    "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                    (item_id, c)
                )

        conn.commit()
        return item_id
    finally:
        conn.close()


def list_items(collection_id, search=None, modified_only=False,
               page=1, per_page=50, sort="title", sort_dir="asc"):
    conn = get_db()
    where = ["i.collection_id=?"]
    params = [collection_id]

    if modified_only:
        where.append("i.is_modified=1")

    if search:
        where.append(
            "(i.title LIKE ? OR i.identifier LIKE ? OR i.creator LIKE ? "
            "OR i.author LIKE ? OR i.publisher LIKE ? OR i.subject LIKE ?)"
        )
        s = f"%{search}%"
        params.extend([s, s, s, s, s, s])

    allowed_sorts = {"title", "identifier", "creator", "author", "publisher",
                     "date", "year", "last_modified", "last_synced"}
    if sort not in allowed_sorts:
        sort = "title"
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    offset = (page - 1) * per_page

    total = conn.execute(
        f"SELECT COUNT(*) FROM items i WHERE {' AND '.join(where)}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT i.*, GROUP_CONCAT(ic.collection, '||') as collections
            FROM items i
            LEFT JOIN item_collections ic ON ic.item_id=i.id
            WHERE {' AND '.join(where)}
            GROUP BY i.id
            ORDER BY i.{sort} {direction} NULLS LAST
            LIMIT ? OFFSET ?""",
        params + [per_page, offset]
    ).fetchall()

    conn.close()
    return {"total": total, "page": page, "per_page": per_page,
            "items": [dict(r) for r in rows]}


def get_item(item_id):
    conn = get_db()
    row = conn.execute(
        """SELECT i.*, GROUP_CONCAT(ic.collection, '||') as collections
           FROM items i
           LEFT JOIN item_collections ic ON ic.item_id=i.id
           WHERE i.id=?
           GROUP BY i.id""",
        (item_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    item = dict(row)
    if item.get("extra_metadata"):
        item["extra_metadata"] = json.loads(item["extra_metadata"])
    if item.get("ia_raw"):
        item["ia_raw"] = json.loads(item["ia_raw"])
    return item


def update_item_fields(item_id, fields: dict):
    """Update specific metadata fields; log changes."""
    conn = get_db()
    try:
        current = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if not current:
            return False

        sets, vals, log_entries = [], [], []
        for field, new_val in fields.items():
            if field in CORE_FIELDS:
                old_val = current[field]
                if str(old_val or "") != str(new_val or ""):
                    sets.append(f"{field}=?")
                    vals.append(new_val)
                    log_entries.append((item_id, field, old_val, new_val))
            elif field == "extra_metadata":
                sets.append("extra_metadata=?")
                vals.append(json.dumps(new_val))

        if sets:
            sets += ["is_modified=1", "is_pushed=0",
                     "last_modified=?"]
            vals.append(datetime.utcnow().isoformat())
            vals.append(item_id)
            conn.execute(f"UPDATE items SET {', '.join(sets)} WHERE id=?", vals)
            conn.executemany(
                "INSERT INTO change_log (item_id, field, old_value, new_value) VALUES (?,?,?,?)",
                log_entries
            )

        # handle item_collections separately
        if "collections" in fields:
            colls = [c.strip() for c in fields["collections"].split("||") if c.strip()]
            conn.execute("DELETE FROM item_collections WHERE item_id=?", (item_id,))
            for c in colls:
                conn.execute(
                    "INSERT OR IGNORE INTO item_collections (item_id, collection) VALUES (?,?)",
                    (item_id, c)
                )

        conn.commit()
        return True
    finally:
        conn.close()


def bulk_update(collection_id, match_field, match_pattern,
                update_fields: dict, exact=False):
    """Bulk update items matching a pattern in match_field."""
    conn = get_db()
    try:
        if exact:
            rows = conn.execute(
                f"SELECT id FROM items WHERE collection_id=? AND {match_field}=?",
                (collection_id, match_pattern)
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT id FROM items WHERE collection_id=? AND {match_field} LIKE ?",
                (collection_id, f"%{match_pattern}%")
            ).fetchall()

        updated = 0
        for row in rows:
            if update_item_fields(row["id"], update_fields):
                updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


def get_item_changes(item_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM change_log WHERE item_id=? ORDER BY changed_at DESC",
        (item_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_pushed(item_id):
    conn = get_db()
    conn.execute(
        "UPDATE items SET is_pushed=1, last_pushed=? WHERE id=?",
        (datetime.utcnow().isoformat(), item_id)
    )
    conn.commit()
    conn.close()


def get_modified_items(collection_id):
    conn = get_db()
    rows = conn.execute(
        """SELECT i.*, GROUP_CONCAT(ic.collection, '||') as collections
           FROM items i
           LEFT JOIN item_collections ic ON ic.item_id=i.id
           WHERE i.collection_id=? AND i.is_modified=1
           GROUP BY i.id
           ORDER BY i.title""",
        (collection_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats(collection_id):
    conn = get_db()
    total = conn.execute(
        "SELECT COUNT(*) FROM items WHERE collection_id=?", (collection_id,)
    ).fetchone()[0]
    modified = conn.execute(
        "SELECT COUNT(*) FROM items WHERE collection_id=? AND is_modified=1",
        (collection_id,)
    ).fetchone()[0]
    pushed = conn.execute(
        "SELECT COUNT(*) FROM items WHERE collection_id=? AND is_pushed=1",
        (collection_id,)
    ).fetchone()[0]
    conn.close()
    return {"total": total, "modified": modified, "pushed": pushed,
            "pending_push": modified - pushed}
