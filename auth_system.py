"""
SOK MetaManager — Authentication System
Role-based access control with login, signup, and user management.

Roles:
  viewer  — read-only access (default)
  editor  — can edit metadata for assigned collections
  admin   — full access, can manage users and collection permissions
"""
import os
import sys
import hashlib
import hmac
import json
import functools

try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False

# ── Password hashing ────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash a password, using bcrypt if available, falling back to salted SHA-256."""
    if HAS_BCRYPT:
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    # Fallback: salted SHA-256
    salt = os.urandom(32).hex()
    h = hashlib.sha256(salt.encode("utf-8") + password.encode("utf-8")).hexdigest()
    return f"sha256${salt}${h}"

def verify_password(stored: str, provided: str) -> bool:
    """Verify a password against its stored hash."""
    if HAS_BCRYPT:
        try:
            return bcrypt.checkpw(provided.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            return False
    # Fallback: parse sha256$salt$hash
    if stored.startswith("sha256$"):
        parts = stored.split("$", 2)
        if len(parts) == 3:
            _, salt, expected = parts
            h = hashlib.sha256(salt.encode("utf-8") + provided.encode("utf-8")).hexdigest()
            return hmac.compare_digest(h, expected)
    return False

# ── Role hierarchy ────────────────────────────────────────────────────────────────

ROLE_HIERARCHY = {"viewer": 0, "editor": 1, "admin": 2}

def role_at_least(user_role: str, required: str) -> bool:
    """Check if user_role meets or exceeds required role."""
    return ROLE_HIERARCHY.get(user_role, 0) >= ROLE_HIERARCHY.get(required, 0)

# ── Database helpers (import db lazily to avoid circular imports) ────────────────

def _get_db():
    import database as db
    return db.get_db()

def create_user(username: str, password: str, role: str = "viewer") -> int:
    """Create a new user. Raises ValueError if username exists."""
    if role not in ROLE_HIERARCHY:
        raise ValueError(f"Invalid role: {role}")
    conn = _get_db()
    try:
        cur = conn.execute(
            "INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
            (username, hash_password(password), role),
        )
        conn.commit()
        return cur.lastrowid
    except Exception:
        # UNIQUE constraint violation etc.
        conn.close()
        raise

def get_user_by_id(user_id: int):
    """Return a user dict by id."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT id, username, role, created_at FROM users WHERE id = ?", (user_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def get_user_by_username(username: str):
    """Return a user dict by username."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT id, username, password, role, created_at FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def list_users():
    """Return all users (without password hashes)."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT id, username, role, created_at FROM users ORDER BY created_at DESC",
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def update_user_role(user_id: int, new_role: str) -> bool:
    """Set the role for a user. Returns True if updated."""
    if new_role not in ROLE_HIERARCHY:
        raise ValueError(f"Invalid role: {new_role}")
    conn = _get_db()
    try:
        cur = conn.execute("UPDATE users SET role = ? WHERE id = ?", (new_role, user_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()

def update_user_password(user_id: int, password: str) -> bool:
    """Set a new password for a user. Returns True if updated."""
    conn = _get_db()
    try:
        cur = conn.execute(
            "UPDATE users SET password = ? WHERE id = ?",
            (hash_password(password), user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()

def delete_user(user_id: int) -> bool:
    """Delete a user. Returns True if deleted."""
    conn = _get_db()
    try:
        # Also clear any collection permissions
        conn.execute("DELETE FROM user_collection_permissions WHERE user_id = ?", (user_id,))
        cur = conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()

# ── Collection-level permissions ────────────────────────────────────────────────

def grant_collection_access(user_id: int, coll_id: int, role: str = "viewer"):
    """Grant a user access to a specific collection with a role.
    If role is 'viewer' and that's also their global role, no override row is needed
    (viewer is the default).  Editors/admins on a collection override the global role.
    """
    if role not in ROLE_HIERARCHY:
        raise ValueError(f"Invalid role: {role}")
    conn = _get_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO user_collection_permissions (user_id, collection_id, role) VALUES (?, ?, ?)",
            (user_id, coll_id, role),
        )
        conn.commit()
    finally:
        conn.close()

def revoke_collection_access(user_id: int, coll_id: int):
    """Remove a user's collection-level permission, reverting to their global role."""
    conn = _get_db()
    try:
        conn.execute(
            "DELETE FROM user_collection_permissions WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        )
        conn.commit()
    finally:
        conn.close()

def get_user_collection_permissions(user_id: int):
    """Return a dict {collection_id: role} for a user's collection overrides."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT collection_id, role FROM user_collection_permissions WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return {r["collection_id"]: r["role"] for r in rows}
    finally:
        conn.close()

def list_collection_users(coll_id: int):
    """Return all users with explicit collection-level access."""
    conn = _get_db()
    try:
        rows = conn.execute('''
            SELECT u.id, u.username, u.role as global_role,
                   p.role as coll_role, u.created_at
            FROM users u
            JOIN user_collection_permissions p ON p.user_id = u.id
            WHERE p.collection_id = ?
            ORDER BY u.created_at
        ''', (coll_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def user_collection_role(user_id: int, coll_id: int):
    """Get a user's effective role for a specific collection.
    Returns the collection override role if it exists, or None to use the global role."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT role FROM user_collection_permissions WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        ).fetchone()
        return row["role"] if row else None
    finally:
        conn.close()

def has_collection_access(user_id: int, coll_id: int, min_role: str = "viewer") -> bool:
    """Check if a user has at least min_role access to a collection."""
    conn = _get_db()
    try:
        # Check for explicit collection permission
        row = conn.execute(
            "SELECT role FROM user_collection_permissions WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        ).fetchone()
        if row:
            return role_at_least(row["role"], min_role)
        # Fall back to global role
        user = get_user_by_id(user_id)
        if user:
            return role_at_least(user["role"], min_role)
        return False
    finally:
        conn.close()

# ── Reviewer scopes: search-pattern-based access to specific item subsets ────────

def get_reviewer_scopes(user_id: int, coll_id: int):
    """Return a list of reviewer scopes (search patterns) for a user on a collection.
    Each scope is a dict: {match_field, match_pattern, match_exact}."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT match_field, match_pattern, match_exact FROM reviewer_scopes "
            "WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        ).fetchall()
        return [{"match_field": r["match_field"], "match_pattern": r["match_pattern"],
                 "match_exact": bool(r["match_exact"])} for r in rows]
    finally:
        conn.close()

def has_item_access(user_id: int, coll_id: int, item: dict) -> bool:
    """Check if a user has access to view a specific item.
    - Admin: full access (scopes ignored)
    - Editor or viewer with reviewer scopes: only items matching at least one scope
    - Editor without scopes: full access
    - Viewer without scopes: no access
    """
    conn = _get_db()
    try:
        user = get_user_by_id(user_id)
        if not user:
            return False
        # Admin always has access
        if user["role"] == "admin":
            return True
        # Check for explicit collection permission
        row = conn.execute(
            "SELECT role FROM user_collection_permissions WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        ).fetchone()
        if row and role_at_least(row["role"], "editor"):
            # Editor with scopes is still restricted by scopes
            scopes = get_reviewer_scopes(user_id, coll_id)
            if not scopes:
                return True  # editor without scopes = full access
            # Editor with scopes = only matching items
            return _matches_any_scope(scopes, item)
        # No editor-level access — check reviewer scopes
        scopes = get_reviewer_scopes(user_id, coll_id)
        if not scopes:
            return False  # viewer without scopes = no access
        return _matches_any_scope(scopes, item)
    finally:
        conn.close()


def _matches_any_scope(scopes, item):
    """Check if an item matches at least one reviewer scope."""
    for scope in scopes:
        field = scope["match_field"]
        pattern = scope["match_pattern"]
        val = (item.get(field) or "").strip()
        if scope["match_exact"]:
            if val == pattern:
                return True
        else:
            if pattern.lower() in val.lower():
                return True
    return False


def can_edit_item(user_id: int, coll_id: int, item: dict) -> bool:
    """Check if a user can edit a specific item."""
    conn = _get_db()
    try:
        user = get_user_by_id(user_id)
        if not user:
            return False
        if user["role"] == "admin":
            return True
        # Check collection-level editor access
        row = conn.execute(
            "SELECT role FROM user_collection_permissions WHERE user_id = ? AND collection_id = ?",
            (user_id, coll_id),
        ).fetchone()
        if row and role_at_least(row["role"], "editor"):
            # Editor with scopes is restricted
            scopes = get_reviewer_scopes(user_id, coll_id)
            if not scopes:
                return True
            return _matches_any_scope(scopes, item)
        # Check global editor role
        if role_at_least(user["role"], "editor"):
            scopes = get_reviewer_scopes(user_id, coll_id)
            if not scopes:
                return True
            return _matches_any_scope(scopes, item)
        # No editor access
        return False
    finally:
        conn.close()


# ── Reviewer scope management ───────────────────────────────────────────────────

def add_reviewer_scope(user_id: int, coll_id: int, field: str, pattern: str,
                       exact: bool = False, name: str = "") -> int:
    """Add a reviewer scope for a user on a collection."""
    conn = _get_db()
    try:
        cur = conn.execute(
            "INSERT INTO reviewer_scopes (user_id, collection_id, match_field, match_pattern, match_exact, name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, coll_id, field, pattern, 1 if exact else 0, name),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

def remove_reviewer_scope(scope_id: int):
    """Remove a reviewer scope by id."""
    conn = _get_db()
    try:
        conn.execute("DELETE FROM reviewer_scopes WHERE id = ?", (scope_id,))
        conn.commit()
    finally:
        conn.close()

def list_reviewer_scopes_for_user(user_id: int):
    """Return all reviewer scopes for a user across all collections."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT rs.id, rs.collection_id, c.name as collection_name, c.identifier, "
            "rs.match_field, rs.match_pattern, rs.match_exact, rs.name, rs.granted_at "
            "FROM reviewer_scopes rs "
            "JOIN collections c ON c.id = rs.collection_id "
            "WHERE rs.user_id = ? ORDER BY rs.granted_at DESC",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def list_reviewer_scopes_for_collection(coll_id: int):
    """Return all reviewer scopes for a collection."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT rs.id, rs.user_id, u.username, rs.match_field, rs.match_pattern, rs.match_exact, rs.name, rs.granted_at "
            "FROM reviewer_scopes rs "
            "JOIN users u ON u.id = rs.user_id "
            "WHERE rs.collection_id = ? ORDER BY rs.granted_at DESC",
            (coll_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

# ── Flask integration decorators (used in app.py) ───────────────────────────────

def login_required(f):
    """Decorator: require the user to be logged in (session['user_id'] set)."""
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        from flask import session, redirect, url_for, request
        if not session.get("user_id"):
            if request.path.startswith("/api/"):
                from flask import jsonify
                return jsonify({"ok": False, "error": "Authentication required"}), 401
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapped

def collection_access_required(min_role: str = "viewer"):
    """Decorator factory: require logged-in user with at least min_role on a collection.
    The collection id must be in kwargs as 'coll_id'."""
    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            from flask import session, request
            coll_id = kwargs.get("coll_id")
            if coll_id is None:
                return f(*args, **kwargs)  # No collection id in route, let route handle
            user_id = session.get("user_id")
            if not user_id:
                if request.path.startswith("/api/"):
                    from flask import jsonify
                    return jsonify({"ok": False, "error": "Authentication required"}), 401
                from flask import redirect, url_for
                return redirect(url_for("login", next=request.path))
            # Check collection-level access
            import auth_system
            if not auth_system.has_collection_access(user_id, int(coll_id), min_role):
                from flask import jsonify
                return err("Insufficient permissions for this collection", 403)
            return f(*args, **kwargs)
        return wrapped
    return decorator

def err(msg, code=400):
    """Import-friendly error helper."""
    from flask import jsonify
    return jsonify({"ok": False, "error": msg}), code
