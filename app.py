"""
SOK IA Metadata Manager — Flask Application
Servants of Knowledge · Internet Archive Metadata Tool
"""
import json
import threading
from flask import Flask, jsonify, request, render_template, abort

import database as db
import ia_service as ia_svc

app = Flask(__name__)

# ── Sync state (per-collection progress) ─────────────────────────────────────
_sync_state: dict = {}   # {collection_id: {status, current, total, error}}


# ── Utility ──────────────────────────────────────────────────────────────────

def ok(data=None, **kwargs):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(kwargs)
    return jsonify(payload)


def err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


# ── Collections ───────────────────────────────────────────────────────────────

@app.route("/api/collections")
def api_list_collections():
    return ok(db.list_collections())


@app.route("/api/collections", methods=["POST"])
def api_add_collection():
    body = request.json or {}
    identifier = (body.get("identifier") or "").strip()
    name = (body.get("name") or identifier).strip()
    description = (body.get("description") or "").strip()
    if not identifier:
        return err("identifier is required")
    try:
        coll = db.add_collection(identifier, name, description)
        return ok(coll), 201
    except Exception as e:
        if "UNIQUE" in str(e):
            return err(f"Collection '{identifier}' already exists")
        return err(str(e))


@app.route("/api/collections/<int:coll_id>", methods=["DELETE"])
def api_delete_collection(coll_id):
    db.delete_collection(coll_id)
    return ok()


@app.route("/api/collections/<int:coll_id>/stats")
def api_collection_stats(coll_id):
    return ok(db.get_stats(coll_id))


# ── Sync ──────────────────────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/sync", methods=["POST"])
def api_sync_collection(coll_id):
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)

    if _sync_state.get(coll_id, {}).get("status") == "running":
        return err("Sync already in progress")

    _sync_state[coll_id] = {"status": "running", "current": 0, "total": 0, "error": None}

    def run_sync():
        try:
            def progress(current, total):
                _sync_state[coll_id]["current"] = current
                _sync_state[coll_id]["total"] = total

            items = ia_svc.fetch_collection_items(coll["identifier"], progress)
            for item in items:
                ident = item.get("identifier")
                if not ident:
                    continue
                db.upsert_item(coll_id, ident, item, ia_raw=item)

            db.update_collection_sync(coll_id, len(items))
            _sync_state[coll_id] = {
                "status": "done",
                "current": len(items),
                "total": len(items),
                "error": None
            }
        except Exception as e:
            _sync_state[coll_id] = {"status": "error", "current": 0,
                                     "total": 0, "error": str(e)}

    t = threading.Thread(target=run_sync, daemon=True)
    t.start()
    return ok({"message": "Sync started"})


@app.route("/api/collections/<int:coll_id>/sync/status")
def api_sync_status(coll_id):
    state = _sync_state.get(coll_id, {"status": "idle"})
    return ok(state)


# ── Items ─────────────────────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/items")
def api_list_items(coll_id):
    search = request.args.get("q", "")
    modified_only = request.args.get("modified_only", "false").lower() == "true"
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    sort = request.args.get("sort", "title")
    sort_dir = request.args.get("sort_dir", "asc")
    result = db.list_items(coll_id, search=search, modified_only=modified_only,
                           page=page, per_page=per_page, sort=sort, sort_dir=sort_dir)
    return ok(result)


@app.route("/api/items/<int:item_id>")
def api_get_item(item_id):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    return ok(item)


@app.route("/api/items/<int:item_id>", methods=["PATCH"])
def api_update_item(item_id):
    body = request.json or {}
    if not body:
        return err("No fields provided")
    success = db.update_item_fields(item_id, body)
    if not success:
        return err("Item not found", 404)
    return ok(db.get_item(item_id))


@app.route("/api/items/<int:item_id>/changes")
def api_item_changes(item_id):
    return ok(db.get_item_changes(item_id))


@app.route("/api/items/<int:item_id>/refresh", methods=["POST"])
def api_refresh_item(item_id):
    """Re-fetch this item's metadata from IA."""
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    fresh = ia_svc.fetch_item_metadata(item["identifier"])
    if "error" in fresh:
        return err(fresh["error"])
    db.upsert_item(item["collection_id"], item["identifier"], fresh, ia_raw=fresh)
    return ok(db.get_item(item_id))


# ── Bulk Edit ────────────────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/bulk-update", methods=["POST"])
def api_bulk_update(coll_id):
    body = request.json or {}
    match_field = body.get("match_field", "")
    match_pattern = body.get("match_pattern", "")
    update_fields = body.get("update_fields", {})
    exact = body.get("exact", False)

    if not match_field or not match_pattern:
        return err("match_field and match_pattern are required")
    if not update_fields:
        return err("update_fields cannot be empty")

    allowed = set(db.CORE_FIELDS)
    for key in update_fields:
        if key not in allowed:
            return err(f"Field '{key}' is not a valid editable field")

    count = db.bulk_update(coll_id, match_field, match_pattern,
                           update_fields, exact=exact)
    return ok({"updated_count": count})


# ── Push to IA ───────────────────────────────────────────────────────────────

PUSHABLE_FIELDS = [
    "title", "alt_title", "creator", "alt_creator", "author", "alt_author",
    "publisher", "alt_publisher", "date", "year", "language", "subject",
    "description", "licenseurl", "mediatype", "volume", "isbn", "source", "notes"
]


@app.route("/api/collections/<int:coll_id>/pending-push")
def api_pending_push(coll_id):
    items = db.get_modified_items(coll_id)
    return ok(items)


@app.route("/api/items/<int:item_id>/push", methods=["POST"])
def api_push_item(item_id):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)

    push_fields = {}
    for f in PUSHABLE_FIELDS:
        val = item.get(f)
        if val is not None:
            push_fields[f] = val

    result = ia_svc.push_metadata_to_ia(item["identifier"], push_fields)
    if result["success"]:
        db.mark_pushed(item_id)
    return ok(result)


@app.route("/api/collections/<int:coll_id>/push-all", methods=["POST"])
def api_push_all(coll_id):
    """Push all modified-but-not-yet-pushed items in collection."""
    items = db.get_modified_items(coll_id)
    results = []
    for item in items:
        if item.get("is_pushed"):
            continue
        push_fields = {f: item.get(f) for f in PUSHABLE_FIELDS if item.get(f)}
        result = ia_svc.push_metadata_to_ia(item["identifier"], push_fields)
        if result["success"]:
            db.mark_pushed(item["id"])
        results.append({
            "identifier": item["identifier"],
            "id": item["id"],
            **result
        })
    return ok(results)


# ── Collection membership ────────────────────────────────────────────────────

@app.route("/api/items/<int:item_id>/collections", methods=["POST"])
def api_add_item_to_collection(item_id):
    body = request.json or {}
    coll_name = (body.get("collection") or "").strip()
    if not coll_name:
        return err("collection name required")
    result = ia_svc.add_to_collection(
        db.get_item(item_id)["identifier"], coll_name
    )
    return ok(result)


@app.route("/api/items/<int:item_id>/collections/<path:coll_name>", methods=["DELETE"])
def api_remove_item_from_collection(item_id, coll_name):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    result = ia_svc.remove_from_collection(item["identifier"], coll_name)
    return ok(result)


# ── System ───────────────────────────────────────────────────────────────────

@app.route("/api/ia-status")
def api_ia_status():
    return ok(ia_svc.check_ia_cli())


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    db.init_db()
    print("\n🕉  SOK IA Metadata Manager")
    print("   http://localhost:5050\n")
    app.run(host="127.0.0.1", port=5050, debug=False)
