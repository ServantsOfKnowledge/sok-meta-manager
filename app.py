"""
SOK MetaManager — Flask Application  (v3)
Servants of Knowledge · IA Metadata + Transliteration Tool

v3: persistent background job queue, per-collection sharded databases,
    sync/operations status page, bulk collection membership, FTS search index
    and disk-cached collection logos.
"""
import csv, io, json, os, re, threading, time
from datetime import datetime
from flask import Flask, jsonify, request, render_template, send_file, Response

import database as db
import ia_service as ia_svc
import transliteration as T

app = Flask(__name__)

# ── Sync mirror (in-memory, for legacy /sync/status + sidebar badges) ────────
_sync_state: dict = {}   # {coll_id: {status, current, total, new_count, error, mode, since, job_id}}


# ── Helpers ───────────────────────────────────────────────────────────────────

def ok(data=None, **kw):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(kw)
    return jsonify(payload)

def err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


# ── Background job worker ─────────────────────────────────────────────────────
# Multiple worker threads run jobs in parallel across DIFFERENT collections
# (claim_next_job skips collections that already have a running job).  A single
# collection still gets one sync at a time.

WORKER_THREADS = 3

_worker_started = False
_worker_lock = threading.Lock()


def start_job_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
    for i in range(WORKER_THREADS):
        threading.Thread(target=_worker_loop, daemon=True,
                         name=f"job-worker-{i+1}").start()


def _worker_loop():
    while True:
        job = db.claim_next_job()
        if job is None:
            time.sleep(1)
            continue
        try:
            _dispatch_job(job)
        except Exception as e:
            db.fail_job(job["id"], str(e))
            db.add_job_log(job["id"], "err", f"Unhandled failure: {e}")


def _dispatch_job(job):
    job_id = job["id"]
    jt     = job["job_type"]
    coll_id = job["collection_id"]
    params = job.get("params") or {}
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            params = {}
    db.add_job_log(job_id, "info", f"Started {jt}")
    if jt == "full-sync":
        _run_sync(job_id, coll_id, params, smart=False)
    elif jt == "smart-sync":
        _run_sync(job_id, coll_id, params, smart=True)
    elif jt == "discover-subs":
        _run_discover_subs(job_id, coll_id, params)
    elif jt == "push-all":
        _run_push_all(job_id, coll_id, params)
    elif jt == "bulk-collection":
        _run_bulk_collection(job_id, coll_id, params)
    elif jt == "fetch-stats":
        _run_fetch_stats(job_id, coll_id, params)
    elif jt == "copy-alt":
        _run_copy_alt(job_id, coll_id, params)
    elif jt == "translit-generate":
        _run_translit_generate(job_id, coll_id, params)
    elif jt == "rebuild-index":
        _run_rebuild_index(job_id, coll_id, params)
    elif jt == "vacuum":
        _run_vacuum(job_id, coll_id, params)
    else:
        raise ValueError(f"Unknown job type: {jt}")


def _job_state(job):
    """Map a job row to the legacy /sync/status shape."""
    if not job:
        return {"status": "idle"}
    params = job.get("params") or {}
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            params = {}
    mode = "smart" if job["job_type"] == "smart-sync" else \
           ("full-noindex" if params.get("include_noindex") else "full")
    return {
        "status":    job["status"],
        "current":   job["current"],
        "total":     job["total"],
        "new_count": job["new_count"],
        "error":     job["error"],
        "mode":      mode,
        "since":     params.get("since"),
        "job_id":    job["id"],
    }


def _sync_done(job_id, coll_id, n, message):
    db.update_job_progress(job_id, n, n)
    db.finish_job(job_id, "done", new_count=n)
    db.add_job_log(job_id, "info", message)
    if coll_id in _sync_state:
        _sync_state[coll_id] = {"status": "done", "current": n, "total": n,
                                "new_count": n, "error": None,
                                "mode": _sync_state[coll_id].get("mode"),
                                "job_id": job_id}


def _run_sync(job_id, coll_id, params, smart=False):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    include_noindex = bool(params.get("include_noindex"))
    mode = "smart" if smart else ("full-noindex" if include_noindex else "full")
    job_row = db.get_job(job_id) or {}
    start_count   = int(job_row.get("current") or params.get("resume_current") or 0)
    resume_cursor = params.get("resume_cursor")
    resume_page   = params.get("resume_page")
    resuming      = bool(resume_cursor or resume_page)
    # A sync with no resume marker is a true start-from-scratch run: begin the
    # progress counter at 0 so it tracks items fetched this run (the portal's
    # local count already reflects previously stored items).
    if not resuming:
        start_count = 0

    _sync_state[coll_id] = {"status": "running", "current": start_count, "total": 0,
                            "new_count": 0, "error": None, "mode": mode,
                            "since": params.get("since"), "job_id": job_id}

    class JobCancelled(Exception):
        pass

    def progress(cur, tot):
        # Honor cancellation even while the fetch phase is still running.
        if db.job_cancel_requested(job_id):
            raise JobCancelled()
        db.update_job_progress(job_id, cur, tot)
        if coll_id in _sync_state:
            _sync_state[coll_id].update({"current": cur, "total": tot})

    def save_cursor(cursor):
        db.set_job_resume(job_id, "resume_cursor", cursor)

    def save_page(page):
        db.set_job_resume(job_id, "resume_page", page)

    try:
        db.add_job_log(job_id, "info",
                       f"Fetching from IA collection '{coll['identifier']}'"
                       + (" (resuming from saved position)" if resuming else ""))
        if smart:
            items = ia_svc.fetch_updated_items(
                coll["identifier"], params.get("since") or coll.get("last_synced"),
                progress, start_cursor=resume_cursor, cursor_callback=save_cursor,
                start_count=start_count)
        elif include_noindex:
            items = ia_svc.fetch_collection_all(
                coll["identifier"], progress, start_cursor=resume_cursor,
                cursor_callback=save_cursor, start_count=start_count)
        else:
            items = ia_svc.fetch_collection_items(
                coll["identifier"], progress, start_page=resume_page,
                page_callback=save_page, start_count=start_count)

        # Stream: persist each item as it arrives so the portal updates live.
        # Cancellation is honoured via the progress callback raising JobCancelled.
        n = start_count
        for item in items:
            if item.get("identifier"):
                db.upsert_item(coll_id, item["identifier"], item, ia_raw=item)
                n += 1

        db.update_collection_sync(coll_id, db.get_stats(coll_id)["total"])
        _sync_done(job_id, coll_id, n, f"Synced {n} item(s) from '{coll['identifier']}'")
        # Auto-discover sub-collections in the background after sync
        db.create_job("discover-subs", coll_id, {},
                      title=f"Discover sub-collections · {coll['name']}")
    except JobCancelled:
        db.finish_job(job_id, "cancelled")
        db.add_job_log(job_id, "warn", "Cancelled by user")
        _sync_state.pop(coll_id, None)
    except Exception as e:
        db.fail_job(job_id, str(e))
        db.add_job_log(job_id, "err", str(e))
        if coll_id in _sync_state:
            _sync_state[coll_id].update({"status": "error", "error": str(e)})


def _run_discover_subs(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    try:
        subs = ia_svc.fetch_sub_collections(coll["identifier"])
        db.clear_sub_collections(coll_id)
        for sub in subs:
            db.upsert_sub_collection(coll_id, sub["ia_id"], sub["name"])
        db.finish_job(job_id, "done", new_count=len(subs))
        db.add_job_log(job_id, "info", f"Discovered {len(subs)} sub-collection(s)")
    except Exception as e:
        db.fail_job(job_id, str(e))
        db.add_job_log(job_id, "err", str(e))


def _run_push_all(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    items = db.get_modified_items(coll_id)
    todo  = [i for i in items if not i.get("is_pushed")]
    db.update_job_progress(job_id, 0, len(todo))
    db.add_job_log(job_id, "info", f"{len(todo)} item(s) to push to Internet Archive")
    ok_count, done, failed = 0, 0, 0
    for item in todo:
        if db.job_cancel_requested(job_id):
            db.finish_job(job_id, "cancelled")
            return
        push_fields = {f: item[f] for f in PUSHABLE if item.get(f)}
        try:
            result = ia_svc.push_metadata_to_ia(item["identifier"], push_fields)
        except Exception as e:
            result = {"success": False, "error": str(e)}
        if result.get("success"):
            db.mark_pushed(item["id"])
            ok_count += 1
        else:
            failed += 1
            db.add_job_log(job_id, "err", f"{item['identifier']}: {result.get('error')}")
        done += 1
        db.update_job_progress(job_id, done, len(todo))
    db.finish_job(job_id, "done", new_count=ok_count)
    db.add_job_log(job_id, "info",
                   f"Pushed {ok_count}/{len(todo)} — {failed} failed")


def _run_bulk_collection(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    action    = params.get("action")
    target    = params.get("collection")
    selection = params.get("selection") or {}

    items = db.select_items(coll_id, selection)
    todo = []
    for it in items:
        in_target = target in (it.get("collections") or [])
        if action == "add" and not in_target:
            todo.append(it)
        elif action == "remove" and in_target:
            todo.append(it)
    skipped = len(items) - len(todo)

    db.update_job_progress(job_id, 0, len(todo))
    db.add_job_log(job_id, "info",
                   f"Matched {len(items)} item(s) — {len(todo)} need {action}, {skipped} skipped")
    ok_count, done, failed = 0, 0, 0
    for it in todo:
        if db.job_cancel_requested(job_id):
            db.finish_job(job_id, "cancelled")
            return
        ident = it["identifier"]
        try:
            if action == "add":
                r = ia_svc.add_to_collection(ident, target)
            else:
                r = ia_svc.remove_from_collection(ident, target)
        except Exception as e:
            r = {"success": False, "error": str(e)}
        if r.get("success"):
            db.set_item_membership(coll_id, it["id"], target, present=(action == "add"))
            ok_count += 1
        else:
            failed += 1
            db.add_job_log(job_id, "err", f"{ident}: {r.get('error')}")
        done += 1
        db.update_job_progress(job_id, done, len(todo))
    db.finish_job(job_id, "done", new_count=ok_count)
    db.add_job_log(job_id, "info",
                   f"{action.title()} finished: {ok_count} ok, {failed} failed, {skipped} skipped")


def _run_fetch_stats(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    job = db.get_job(job_id) or {}
    jparams = job.get("params") or {}
    if isinstance(jparams, str):
        try:
            jparams = json.loads(jparams)
        except Exception:
            jparams = {}

    total = jparams.get("total")
    if total is None:
        total = db.get_stats(coll_id).get("total") or 0
        db.set_job_resume(job_id, "total", total)

    after_id = jparams.get("last_id")
    done = int(jparams.get("done") or 0)
    db.update_job_progress(job_id, done, total)
    db.add_job_log(job_id, "info",
                   f"Fetching views/downloads for {total} item(s)"
                   + (", resuming from item " + str(after_id) if after_id else ""))

    while True:
        if db.job_cancel_requested(job_id):
            db.finish_job(job_id, "cancelled")
            return
        batch = db.get_item_identifiers(coll_id, after_id=after_id, limit=100)
        if not batch:
            break
        try:
            stats_map = ia_svc.fetch_item_stats([it["identifier"] for it in batch])
        except Exception as e:
            db.fail_job(job_id, str(e))
            return
        # Only persist rows IA actually has data for, so a transient miss
        # never zeroes out previously captured counts.
        stats_map = {k: v for k, v in stats_map.items() if v.get("have_data")}
        if stats_map:
            db.update_item_stats_batch(coll_id, stats_map)
        after_id = batch[-1]["id"]
        done += len(batch)
        db.set_job_resume(job_id, "last_id", after_id)
        db.set_job_resume(job_id, "done", done)
        db.update_job_progress(job_id, done, total)
        time.sleep(0.05)

    db.finish_job(job_id, "done", new_count=done)
    db.add_job_log(job_id, "info", f"Updated views/downloads for {done} item(s)")


def _run_copy_alt(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    lang_code = params.get("lang_code")
    item_id   = params.get("item_id")
    if item_id:
        item = db.get_item(item_id)
        items = [item] if (item and item.get("collection_id") or
                           db._coll_id_from_item_id(item_id)) == coll_id else []
    else:
        items = db.get_items_for_transliteration(coll_id, lang_code, status_filter="none") \
                if lang_code else _all_indian_none(coll_id)
    db.update_job_progress(job_id, 0, len(items))
    updated, done = 0, 0
    for item in items:
        if db.job_cancel_requested(job_id):
            db.finish_job(job_id, "cancelled")
            return
        if not T.is_indian(item.get("detected_language")):
            continue
        updates = T.build_alt_copy(item)
        if updates:
            updates["translit_status"] = "copied"
            db.update_item_fields(item["id"], updates)
        else:
            db.update_item_fields(item["id"], {"translit_status": "copied"})
        updated += 1
        done += 1
        db.update_job_progress(job_id, done, len(items))
    db.finish_job(job_id, "done", new_count=updated)
    db.add_job_log(job_id, "info", f"Copied alt_ values for {updated} item(s)")


def _run_translit_generate(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    lang_code    = params.get("lang_code")
    input_scheme = params.get("scheme", "itrans")
    if not lang_code:
        raise ValueError("lang_code is required")
    if not T.check_lib_available():
        raise RuntimeError("indic-transliteration library not installed. Run: pip install indic-transliteration")
    items = db.get_items_for_transliteration(coll_id, lang_code, status_filter="copied")
    db.update_job_progress(job_id, 0, len(items))
    done = 0
    for item in items:
        if db.job_cancel_requested(job_id):
            db.finish_job(job_id, "cancelled")
            return
        updates = {"translit_status": "generated"}
        for field in T.TRANSLIT_FIELDS:
            val = (item.get(field) or "").strip()
            if not val:
                continue
            translit, conf = T.transliterate_text(val, lang_code, input_scheme)
            if translit and translit != val:
                updates[field] = translit
        db.update_item_fields(item["id"], updates)
        done += 1
        db.update_job_progress(job_id, done, len(items))
    db.finish_job(job_id, "done", new_count=done)
    db.add_job_log(job_id, "info", f"Generated transliterations for {done} item(s)")


def _run_rebuild_index(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    n = db.fts_rebuild_coll(coll_id)
    db.finish_job(job_id, "done", new_count=n)
    db.add_job_log(job_id, "info", f"Rebuilt search index — {n} item(s) indexed")


def _run_vacuum(job_id, coll_id, params):
    coll = db.get_collection(coll_id)
    if not coll:
        db.fail_job(job_id, "Collection no longer exists")
        return
    db.vacuum_coll(coll_id)
    db.finish_job(job_id, "done")
    db.add_job_log(job_id, "info", "Database vacuumed")


# ── Collections ───────────────────────────────────────────────────────────────

@app.route("/api/collections")
def api_list_collections():
    return ok(db.list_collections())

@app.route("/api/collections/tree")
def api_collections_tree():
    """Return the full collection hierarchy with sub-collection item counts."""
    return ok(db.get_collections_tree())

@app.route("/api/collections", methods=["POST"])
def api_add_collection():
    body = request.get_json(silent=True) or {}
    identifier  = (body.get("identifier") or "").strip()
    name        = (body.get("name") or identifier).strip()
    description = (body.get("description") or "").strip()
    if not identifier:
        return err("identifier is required")
    try:
        coll = db.add_collection(identifier, name, description)
        return ok(coll), 201
    except Exception as e:
        return err("Collection already exists" if "UNIQUE" in str(e) else str(e))

@app.route("/api/collections/<int:coll_id>", methods=["DELETE"])
def api_delete_collection(coll_id):
    db.delete_collection(coll_id)
    return ok()

@app.route("/api/collections/<int:coll_id>/stats")
def api_collection_stats(coll_id):
    ia_coll = request.args.get("ia_collection") or None
    return ok(db.get_stats(coll_id, ia_coll))


# ── Sync & background jobs ────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/discover-subs", methods=["POST"])
def api_discover_subs(coll_id):
    """Queue sub-collection discovery for a super-collection."""
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    job_id = db.create_job("discover-subs", coll_id, {},
                           title=f"Discover sub-collections · {coll['name']}")
    return ok({"job_id": job_id}), 202


@app.route("/api/collections/<int:coll_id>/sync", methods=["POST"])
def api_sync_collection(coll_id):
    """
    Queue a full sync.  Pass JSON body {"include_noindex": true} to use the
    authenticated scrape API (fetches hidden items the account has access to).
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body = request.get_json(silent=True) or {}
    include_noindex = bool(body.get("include_noindex", False))
    mode = "full-noindex" if include_noindex else "full"
    # Only one pending full sync per collection: supersede older queued ones
    db.cancel_queued_syncs(coll_id, "full-sync")
    params = {"include_noindex": include_noindex}
    # Resume from the most recent failed sync of the same type if available
    resume = db.latest_sync_resume(coll_id, "full-sync", include_noindex=include_noindex)
    if resume:
        for k in ("resume_cursor", "resume_page"):
            if k in resume:
                params[k] = resume[k]
        params["resume_current"] = resume.get("current", 0)
    job_id = db.create_job(
        "full-sync", coll_id, params,
        title=f"Full sync · {coll['name']} · {'incl. hidden' if include_noindex else 'public'}",
    )
    _sync_state[coll_id] = {"status": "queued", "current": 0, "total": 0,
                            "new_count": 0, "error": None, "mode": mode, "job_id": job_id}
    return ok({"job_id": job_id,
               "message": "Full sync queued",
               "include_noindex": include_noindex}), 202


@app.route("/api/collections/<int:coll_id>/smart-sync", methods=["POST"])
def api_smart_sync(coll_id):
    """
    Queue a smart sync — fetch only items changed on IA since the collection's
    last_synced date.  Pass {"include_noindex": true} for hidden items too.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body  = request.get_json(silent=True) or {}
    since = body.get("since") or coll.get("last_synced")
    # Only one pending smart sync per collection: supersede older queued ones
    db.cancel_queued_syncs(coll_id, "smart-sync")
    params = {"since": since, "include_noindex": body.get("include_noindex", False)}
    # Resume from the most recent failed sync of the same type if available
    resume = db.latest_sync_resume(coll_id, "smart-sync")
    if resume:
        for k in ("resume_cursor", "resume_page"):
            if k in resume:
                params[k] = resume[k]
        params["resume_current"] = resume.get("current", 0)
    job_id = db.create_job(
        "smart-sync", coll_id, params,
        title=f"Smart sync · {coll['name']}",
    )
    _sync_state[coll_id] = {"status": "queued", "current": 0, "total": 0,
                            "new_count": 0, "error": None, "mode": "smart",
                            "since": since, "job_id": job_id}
    return ok({"job_id": job_id, "message": "Smart sync queued", "since": since}), 202


@app.route("/api/collections/<int:coll_id>/sync/status")
def api_sync_status(coll_id):
    job = db.latest_job_for_collection(coll_id, types=("full-sync", "smart-sync"))
    if not job:
        return ok({"status": "idle"})
    return ok(_job_state(job))


# ── Job queue management ──────────────────────────────────────────────────────

@app.route("/api/jobs", methods=["POST"])
def api_enqueue_job():
    """Generic job enqueue (used for retries and custom operations)."""
    body    = request.get_json(silent=True) or {}
    jt      = (body.get("job_type") or "").strip()
    coll_id = body.get("collection_id")
    params  = body.get("params") or {}
    title   = body.get("title") or ""
    if not jt:
        return err("job_type is required")
    if coll_id is not None:
        coll = db.get_collection(coll_id)
        if not coll:
            return err("Collection not found", 404)
    job_id = db.create_job(jt, coll_id, params, title=title)
    return ok({"job_id": job_id}), 202


@app.route("/api/jobs")
def api_list_jobs():
    coll_id = request.args.get("collection_id", type=int)
    return ok({
        "jobs":    db.list_jobs(collection_id=coll_id, limit=100),
        "summary": db.job_counts(),
    })


@app.route("/api/jobs/<int:job_id>")
def api_get_job(job_id):
    job = db.get_job(job_id)
    return ok(job) if job else err("Job not found", 404)


@app.route("/api/jobs/<int:job_id>/log")
def api_job_log(job_id):
    return ok(db.get_job_log(job_id))


@app.route("/api/jobs/<int:job_id>/cancel", methods=["POST"])
def api_cancel_job(job_id):
    db.cancel_job(job_id)
    return ok()


# ── Per-collection database management ────────────────────────────────────────

@app.route("/api/databases")
def api_databases():
    out = []
    for coll in db.list_collections():
        try:
            out.append(db.collection_db_info(coll["id"]))
        except Exception as e:
            out.append({"id": coll["id"], "name": coll["name"],
                        "identifier": coll["identifier"], "error": str(e)})
    return ok(out)


@app.route("/api/databases/<int:coll_id>/rebuild-index", methods=["POST"])
def api_rebuild_index(coll_id):
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    job_id = db.create_job("rebuild-index", coll_id, {},
                           title=f"Rebuild search index · {coll['name']}")
    return ok({"job_id": job_id}), 202


@app.route("/api/databases/<int:coll_id>/vacuum", methods=["POST"])
def api_vacuum(coll_id):
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    job_id = db.create_job("vacuum", coll_id, {},
                           title=f"Vacuum database · {coll['name']}")
    return ok({"job_id": job_id}), 202


# ── Items ─────────────────────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/items")
def api_list_items(coll_id):
    result = db.list_items(
        coll_id,
        search          = request.args.get("q", ""),
        modified_only   = request.args.get("modified_only", "false").lower() == "true",
        lang_code       = request.args.get("lang") or None,
        translit_status = request.args.get("tstatus") or None,
        ia_collection   = request.args.get("ia_collection") or None,
        ia_collection_not = request.args.get("ia_collection_not") or None,
        page            = int(request.args.get("page", 1)),
        per_page        = int(request.args.get("per_page", 50)),
        sort            = request.args.get("sort", "title"),
        sort_dir        = request.args.get("sort_dir", "asc"),
    )
    return ok(result)

@app.route("/api/items/<int:item_id>")
def api_get_item(item_id):
    item = db.get_item(item_id)
    return ok(item) if item else err("Item not found", 404)

@app.route("/api/items/<int:item_id>", methods=["PATCH"])
def api_update_item(item_id):
    body = request.get_json(silent=True) or {}
    if not body:
        return err("No fields provided")
    db.update_item_fields(item_id, body)
    return ok(db.get_item(item_id))

@app.route("/api/items/<int:item_id>/changes")
def api_item_changes(item_id):
    return ok(db.get_item_changes(item_id))

@app.route("/api/items/<int:item_id>/refresh", methods=["POST"])
def api_refresh_item(item_id):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    fresh = ia_svc.fetch_item_metadata(item["identifier"])
    if "error" in (fresh or {}):
        return err(fresh["error"])
    db.upsert_item(item["collection_id"] or db._coll_id_from_item_id(item_id),
                   item["identifier"], fresh, ia_raw=fresh)
    return ok(db.get_item(item_id))

@app.route("/api/items/<int:item_id>/thumbnail")
def api_item_thumbnail(item_id):
    item = db.get_item(item_id)
    if not item:
        return err("Not found", 404)
    return ok({"url": ia_svc.thumbnail_url(item["identifier"])})


LOGO_CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "cache", "logos")


@app.route("/api/collections/<int:coll_id>/logo")
def api_collection_logo(coll_id):
    """
    Proxy the IA collection logo through the authenticated IA session so that
    private/noindex collection thumbnails load correctly in the browser.
    Disk-cached for 1 hour to avoid repeated round-trips.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Not found", 404)
    ident = coll["identifier"]
    safe  = re.sub(r"[^A-Za-z0-9_.-]", "_", ident)
    img_path  = os.path.join(LOGO_CACHE_DIR, safe + ".img")
    meta_path = os.path.join(LOGO_CACHE_DIR, safe + ".meta")
    try:
        if (os.path.exists(img_path) and os.path.exists(meta_path)
                and time.time() - os.path.getmtime(img_path) < 3600):
            with open(meta_path) as f:
                content_type = f.read().strip() or "image/jpeg"
            with open(img_path, "rb") as f:
                img_bytes = f.read()
            return Response(img_bytes, content_type=content_type,
                            headers={"Cache-Control": "public, max-age=3600"})
    except OSError:
        pass

    try:
        session = ia_svc._get_session()
        url  = f"https://archive.org/services/img/{ident}"
        resp = session.get(url, timeout=15, stream=True)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg")
        img_bytes    = resp.raw.read(decode_content=True)
        try:
            os.makedirs(LOGO_CACHE_DIR, exist_ok=True)
            with open(img_path, "wb") as f:
                f.write(img_bytes)
            with open(meta_path, "w") as f:
                f.write(content_type)
        except OSError:
            pass
        return Response(img_bytes, content_type=content_type,
                        headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        return err(str(e), 502)


# ── Bulk edit ─────────────────────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/bulk-update", methods=["POST"])
def api_bulk_update(coll_id):
    body          = request.get_json(silent=True) or {}
    match_field   = body.get("match_field", "")
    match_pattern = body.get("match_pattern", "")
    update_fields = body.get("update_fields", {})
    exact         = body.get("exact", False)
    if not match_field or not match_pattern:
        return err("match_field and match_pattern are required")
    if not update_fields:
        return err("update_fields cannot be empty")
    count = db.bulk_update(coll_id, match_field, match_pattern, update_fields, exact=exact)
    return ok({"updated_count": count})


# ── Bulk collection membership ────────────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/bulk-collection", methods=["POST"])
def api_bulk_collection(coll_id):
    """
    Queue adding or removing a batch of items to/from an IA collection.
    Selection keys mirror the item filters: match_all, match_field+match_pattern,
    q, lang, tstatus, ia_collection, modified_only.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body   = request.get_json(silent=True) or {}
    action = (body.get("action") or "").strip()
    target = (body.get("collection") or "").strip()
    if action not in ("add", "remove"):
        return err('action must be "add" or "remove"')
    if not target:
        return err("collection name is required")
    selection = body.get("selection") or {}
    try:
        matched = db.count_items(coll_id, selection)
    except Exception as e:
        return err(f"Invalid selection: {e}")
    job_id = db.create_job(
        "bulk-collection", coll_id,
        {"action": action, "collection": target, "selection": selection},
        title=f"Bulk {action}: {target} · {coll['name']}",
    )
    db.add_job_log(job_id, "info",
                   f"{matched} item(s) matched; queued {action} to '{target}'")
    return ok({"job_id": job_id, "matched": matched}), 202


@app.route("/api/collections/<int:coll_id>/bulk-count", methods=["POST"])
def api_bulk_count(coll_id):
    """Live estimate of how many items a bulk selection matches (no job queued)."""
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body = request.get_json(silent=True) or {}
    selection = body.get("selection") or {}
    try:
        matched = db.count_items(coll_id, selection)
    except Exception as e:
        return err(f"Invalid selection: {e}")
    return ok({"matched": matched})


@app.route("/api/collections/<int:coll_id>/fetch-stats", methods=["POST"])
def api_fetch_stats(coll_id):
    """Queue fetching view/download counts from IA for every item in the
    collection's local database."""
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    job_id = db.create_job("fetch-stats", coll_id, {},
                           title=f"Fetch views/downloads · {coll['name']}")
    return ok({"job_id": job_id}), 202


@app.route("/api/collections/<int:coll_id>/items/bulk-fields", methods=["POST"])
def api_bulk_fields_by_ids(coll_id):
    """
    Apply metadata field edits (subject, publisher, …) to an explicit list of
    item ids (checkbox selection on the listing page). Body: {ids, fields}.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body   = request.get_json(silent=True) or {}
    ids    = body.get("ids") or []
    fields = body.get("fields") or {}
    if not isinstance(ids, list) or not ids:
        return err("ids (array of item ids) is required")
    if not isinstance(fields, dict) or not fields:
        return err("fields cannot be empty")
    try:
        count = db.bulk_update_by_ids(coll_id, [int(i) for i in ids], fields)
    except Exception as e:
        return err(f"Update failed: {e}")
    return ok({"updated_count": count})


@app.route("/api/collections/<int:coll_id>/collections")
def api_collection_names(coll_id):
    """List the IA collections these items belong to, with item counts,
    ordered by population. Query params: min_count (default 2), limit
    (default 500) to drop one-off per-item collections."""
    try:
        min_count = int(request.args.get("min_count", 2))
        limit     = int(request.args.get("limit", 500))
    except ValueError:
        return err("min_count and limit must be integers")
    return ok(db.get_collection_names(coll_id, min_count=min_count, limit=limit))


# ── Push to IA ────────────────────────────────────────────────────────────────

PUSHABLE = [
    "title","alt_title","creator","alt_creator","author","alt_author",
    "publisher","alt_publisher","date","year","language","subject",
    "description","licenseurl","mediatype","volume","isbn","source","notes"
]

@app.route("/api/collections/<int:coll_id>/pending-push")
def api_pending_push(coll_id):
    return ok(db.get_modified_items(coll_id))

@app.route("/api/items/<int:item_id>/push", methods=["POST"])
def api_push_item(item_id):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    push_fields = {f: item[f] for f in PUSHABLE if item.get(f)}
    result = ia_svc.push_metadata_to_ia(item["identifier"], push_fields)
    if result["success"]:
        db.mark_pushed(item_id)
    return ok(result)

@app.route("/api/collections/<int:coll_id>/push-all", methods=["POST"])
def api_push_all(coll_id):
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    items   = db.get_modified_items(coll_id)
    pending = [i for i in items if not i.get("is_pushed")]
    job_id  = db.create_job("push-all", coll_id, {},
                            title=f"Push {len(pending)} item(s) to IA · {coll['name']}")
    return ok({"job_id": job_id, "pending": len(pending)}), 202


# ── Collection membership (single item) ───────────────────────────────────────

@app.route("/api/items/<int:item_id>/collections", methods=["POST"])
def api_add_item_to_collection(item_id):
    body  = request.get_json(silent=True) or {}
    cname = (body.get("collection") or "").strip()
    if not cname:
        return err("collection name required")
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    result = ia_svc.add_to_collection(item["identifier"], cname)
    if result.get("success"):
        db.set_item_membership(db._coll_id_from_item_id(item_id), item_id, cname, present=True)
    return ok(result)

@app.route("/api/items/<int:item_id>/collections/<path:cname>", methods=["DELETE"])
def api_remove_item_from_collection(item_id, cname):
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    result = ia_svc.remove_from_collection(item["identifier"], cname)
    if result.get("success"):
        db.set_item_membership(db._coll_id_from_item_id(item_id), item_id, cname, present=False)
    return ok(result)


# ── Languages & transliteration pipeline ─────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/languages")
def api_languages(coll_id):
    """Language breakdown with transliteration stage counts, scoped to the
    active sub-collection and/or the Modified tab."""
    breakdown = db.get_language_breakdown(
        coll_id,
        ia_collection=request.args.get("ia_collection") or None,
        modified_only=request.args.get("modified_only", "false").lower() == "true",
    )
    for lang in breakdown:
        lang["label"] = T.get_language_label(lang["code"])
        lang["is_indian"] = T.is_indian(lang["code"])
    return ok(breakdown)


@app.route("/api/collections/<int:coll_id>/translit/copy-alt", methods=["POST"])
def api_copy_alt(coll_id):
    """
    Queue: for all non-English items with translit_status='none', copy English
    values to alt_ fields and advance status → 'copied'.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body      = request.get_json(silent=True) or {}
    lang_code = body.get("lang_code")
    item_id   = body.get("item_id")
    if item_id:
        params = {"lang_code": lang_code, "item_id": item_id}
        title  = f"Copy alt_ · {coll['name']} · item #{item_id}"
    else:
        params = {"lang_code": lang_code}
        title  = f"Copy alt_ values · {coll['name']}" + (f" · {lang_code}" if lang_code else "")
    job_id = db.create_job("copy-alt", coll_id, params, title=title)
    return ok({"job_id": job_id}), 202


@app.route("/api/collections/<int:coll_id>/translit/generate", methods=["POST"])
def api_generate_translit(coll_id):
    """
    Queue the transliteration engine on items with status='copied'.
    Sets main fields to the transliterated form; status → 'generated'.
    """
    coll = db.get_collection(coll_id)
    if not coll:
        return err("Collection not found", 404)
    body         = request.get_json(silent=True) or {}
    lang_code    = body.get("lang_code")
    input_scheme = body.get("scheme", "itrans")
    if not lang_code:
        return err("lang_code is required")
    job_id = db.create_job(
        "translit-generate", coll_id,
        {"lang_code": lang_code, "scheme": input_scheme},
        title=f"Generate transliteration · {coll['name']} · {lang_code}",
    )
    return ok({"job_id": job_id}), 202


@app.route("/api/items/<int:item_id>/translit", methods=["POST"])
def api_translit_single(item_id):
    """Transliterate a single item on demand."""
    body         = request.get_json(silent=True) or {}
    input_scheme = body.get("scheme", "itrans")
    item         = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    lang = item.get("detected_language")
    if not T.is_indian(lang):
        return err("Item language is not an Indian language")
    if not T.check_lib_available():
        return err("indic-transliteration not installed")

    updates       = {"translit_status": "generated"}
    field_results = []
    for field in T.TRANSLIT_FIELDS:
        val = (item.get(field) or "").strip()
        if not val:
            continue
        translit, conf = T.transliterate_text(val, lang, input_scheme)
        if translit and translit != val:
            updates[field] = translit
            field_results.append({"field": field, "original": val,
                                  "transliterated": translit, "confidence": conf})
    db.update_item_fields(item_id, updates)
    return ok({"fields": field_results, "item": db.get_item(item_id)})


@app.route("/api/items/<int:item_id>/translit/approve", methods=["POST"])
def api_approve_translit(item_id):
    """Mark transliteration as reviewed/finalized."""
    body = request.get_json(silent=True) or {}
    allowed = set(db.CORE_FIELDS)
    updates = {k: v for k, v in body.items() if k in allowed}
    updates["translit_status"] = "finalized"
    db.update_item_fields(item_id, updates)
    return ok(db.get_item(item_id))


@app.route("/api/items/<int:item_id>/revert", methods=["POST"])
def api_revert_item(item_id):
    """Revert all local edits — restores values from the last IA sync snapshot."""
    result = db.revert_item_to_ia(item_id)
    if result is None:
        return err("No IA snapshot stored for this item. Refresh from IA first.")
    return ok(db.get_item(item_id))


@app.route("/api/items/<int:item_id>/revert-change/<int:change_id>", methods=["POST"])
def api_revert_change(item_id, change_id):
    """Revert a single field change by its change_log ID."""
    result = db.revert_item_field(item_id, change_id)
    if result is None:
        return err("Change not found", 404)
    return ok({"reverted": result, "item": db.get_item(item_id)})


@app.route("/api/items/<int:item_id>/translit/reset", methods=["POST"])
def api_reset_translit(item_id):
    """Reset a single item's transliteration back to English (alt_ as source of truth)."""
    item = db.get_item(item_id)
    if not item:
        return err("Item not found", 404)
    revert = {}
    for src, dst in T.ALT_FIELD_MAP.items():
        alt_val = (item.get(dst) or "").strip()
        if alt_val:
            revert[src] = alt_val
    revert["translit_status"] = "none"
    db.update_item_fields(item_id, revert)
    return ok(db.get_item(item_id))


# ── Volunteer review export / import ─────────────────────────────────────────

@app.route("/api/collections/<int:coll_id>/review/export")
def api_review_export(coll_id):
    """
    Export a review CSV for a language batch.
    Columns: item_id, identifier, language, field, english_value,
             draft_transliteration, volunteer_correction, notes
    """
    lang_code = request.args.get("lang", "")
    if not lang_code:
        return err("lang query parameter required")

    coll  = db.get_collection(coll_id)
    items = db.get_items_for_transliteration(coll_id, lang_code)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "item_id", "identifier", "language", "field",
        "english_value", "draft_transliteration",
        "volunteer_correction", "notes"
    ])

    for item in items:
        lang_label = T.get_language_label(lang_code)
        for field in T.TRANSLIT_FIELDS:
            alt_field = T.ALT_FIELD_MAP.get(field)
            english   = (item.get(alt_field) or item.get(field) or "").strip()
            draft     = (item.get(field) or "").strip() if item.get("translit_status") in ("generated","finalized") else ""
            if english:
                writer.writerow([
                    item["id"], item["identifier"], lang_label, field,
                    english, draft, "", ""
                ])

    db.create_review_batch(coll_id, lang_code,
                           f"review_{lang_code}_{datetime.utcnow().strftime('%Y%m%d')}.csv",
                           len(items))
    output.seek(0)
    fname = f"SOK_Review_{T.get_language_label(lang_code)}_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=fname
    )


@app.route("/api/collections/<int:coll_id>/review/import", methods=["POST"])
def api_review_import(coll_id):
    """
    Import a volunteer-reviewed CSV back.
    Reads volunteer_correction column; if non-empty, applies it to the field.
    Returns a preview list for staff to approve before committing.
    """
    commit = request.args.get("commit", "false").lower() == "true"
    file   = request.files.get("file")
    if not file:
        return err("No file uploaded")

    content = file.read().decode("utf-8-sig")
    reader  = csv.DictReader(io.StringIO(content))

    changes = []
    for row in reader:
        correction = (row.get("volunteer_correction") or "").strip()
        if not correction:
            continue
        item_id = int(row.get("item_id", 0))
        field   = row.get("field", "").strip()
        if not item_id or field not in T.TRANSLIT_FIELDS:
            continue
        changes.append({
            "item_id":    item_id,
            "identifier": row.get("identifier", ""),
            "field":      field,
            "original":   row.get("draft_transliteration", ""),
            "correction": correction,
            "notes":      row.get("notes", ""),
        })

    if commit:
        for ch in changes:
            db.update_item_fields(ch["item_id"], {
                ch["field"]: ch["correction"],
                "translit_status": "reviewed",
            })

    return ok({"changes": changes, "count": len(changes), "committed": commit})


@app.route("/api/collections/<int:coll_id>/review/batches")
def api_review_batches(coll_id):
    return ok(db.list_review_batches(coll_id))


# ── Transliteration schemes list ──────────────────────────────────────────────

@app.route("/api/translit/schemes")
def api_schemes():
    return ok([{"key": v, "label": k} for k, v in T.INPUT_SCHEMES.items()])


# ── System ────────────────────────────────────────────────────────────────────

@app.route("/api/ia-status")
def api_ia_status():
    status = ia_svc.check_ia_cli()
    status["translit_lib"] = T.check_lib_available()
    return ok(status)


@app.route("/api/health")
def api_health():
    """Comprehensive system diagnostics — DB, IA CLI, job queue, collection stats."""
    import sqlite3

    result = {
        "db": {}, "ia": {}, "translit": {}, "sync": {},
        "collections": [], "errors": [],
    }

    # ── Database ──────────────────────────────────────────────────────────
    try:
        conn = db.get_db()
        result["db"]["status"] = "ok"
        result["db"]["path"]   = db.DB_PATH
        result["db"]["size_kb"] = round(
            os.path.getsize(db.DB_PATH) / 1024, 1
        ) if os.path.exists(db.DB_PATH) else 0

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        result["db"]["tables"] = tables
        result["db"]["collection_count"] = conn.execute(
            "SELECT COUNT(*) FROM collections"
        ).fetchone()[0]
        result["db"]["sub_collection_count"] = conn.execute(
            "SELECT COUNT(*) FROM ia_sub_collections"
        ).fetchone()[0]
        conn.close()

        # Item count is the sum across per-collection DBs
        total_items = sum(db.get_stats(c["id"])["total"] for c in db.list_collections())
        result["db"]["item_count"] = total_items
        result["db"]["collection_db_dir"] = db.COLL_DB_DIR
    except Exception as e:
        result["db"]["status"] = "error"
        result["db"]["error"]  = str(e)
        result["errors"].append(f"DB: {e}")

    # ── IA CLI ────────────────────────────────────────────────────────────
    try:
        ia_info = ia_svc.check_ia_cli()
        result["ia"] = ia_info
        import subprocess
        chk = subprocess.run(
            ["ia", "list", "--help"],
            capture_output=True, text=True, timeout=10
        )
        result["ia"]["cli_reachable"] = chk.returncode == 0
    except Exception as e:
        result["ia"]["status"] = "error"
        result["ia"]["error"]  = str(e)

    # ── Transliteration library ───────────────────────────────────────────
    result["translit"]["available"] = T.check_lib_available()

    # ── Active syncs / job queue ──────────────────────────────────────────
    running = {}
    for job in db.list_jobs(limit=50):
        if job["status"] in ("running", "queued") and job["collection_id"]:
            running[str(job["collection_id"])] = _job_state(job)
    result["sync"] = running
    result["job_counts"] = db.job_counts()

    # ── Per-collection stats ──────────────────────────────────────────────
    try:
        for coll in db.list_collections():
            stats = db.get_stats(coll["id"])
            result["collections"].append({
                "id":         coll["id"],
                "name":       coll["name"],
                "identifier": coll["identifier"],
                "last_synced": coll.get("last_synced"),
                **stats,
            })
    except Exception as e:
        result["errors"].append(f"Collections: {e}")

    return ok(result)


@app.route("/")
def index():
    return render_template("index.html")


# ── Internal helper ───────────────────────────────────────────────────────────

def _all_indian_none(coll_id):
    """Get all Indian-language items with translit_status='none'."""
    conn = db.get_coll_db(coll_id)
    try:
        rows = conn.execute(
            "SELECT * FROM items WHERE translit_status='none'"
        ).fetchall()
        return [dict(r) for r in rows if T.is_indian(r["detected_language"])]
    finally:
        conn.close()


if __name__ == "__main__":
    db.init_db()
    db.mark_interrupted_jobs()
    start_job_worker()
    print("\n🕉  SOK MetaManager")
    print("   http://localhost:5050\n")
    app.run(host="127.0.0.1", port=5050, debug=False)