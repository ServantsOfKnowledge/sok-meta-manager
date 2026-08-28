# Authentication & Authorization

SOK MetaManager includes a complete role-based authentication and authorization system that controls who can access the app, what collections they can see, and which items they can edit.

## Quick Start

### Default Accounts

| Username | Password | Role | Scope |
|---|---|---|---|
| `admin` | `newpass456` | admin | Full access to all collections and items |
| `sanskrit_reviewer` | `revpass123` | viewer | Only Sanskrit items (`detected_language = "san"`) in collection 15 |

### Creating the First Admin User

If the database is empty, create an admin account via Python:

```bash
python3 -c "
import database as db
import auth_system
db.init_db()
auth_system.create_user('admin', 'yourpassword', 'admin')
"
```

## Roles

| Role | Global Permissions |
|---|---|
| **viewer** | Read-only access to assigned collections and matching items |
| **editor** | Can edit items within assigned collections and matching scopes |
| **admin** | Full access to everything — manage users, collections, create scopes |

## Access Model

Access is evaluated in three layers:

### 1. Global Role
Every user has a **global role** (viewer/editor/admin) that acts as a default.

### 2. Collection-Level Permission (Override)
Admins can grant a user a specific role for a specific collection, overriding their global role.

```
POST /api/collections/<coll_id>/permissions
Body: { "user_id": 4, "role": "editor" }
```

### 3. Reviewer Scopes (Item-Level Filtering)
Reviewer scopes restrict a user to only see/edit items matching specific field+pattern criteria. This is the most granular level of access control.

```
POST /api/collections/<coll_id>/reviewers
Body: {
  "user_id": 4,
  "match_field": "detected_language",
  "match_pattern": "san",
  "match_exact": true,
  "name": "Sanskrit editors"
}
```

#### Scope Evaluation Logic

- **Admin**: Full access to all items (scopes are ignored)
- **Editor without scopes**: Full edit access to all items in assigned collections
- **Editor with scopes**: Edit access only to items matching their scopes
- **Viewer with scopes**: Read/edit access only to items matching their scopes
- **Viewer without scopes**: No access to items

## API Endpoints

### Authentication

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/login` | public | Login with username + password |
| POST | `/api/auth/logout` | any | Clear session |
| POST | `/api/auth/signup` | admin | Create new user (first user always "viewer" if not admin) |
| GET | `/api/auth/me` | any | Get current user info |

### User Management (admin only)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/users` | List all users |
| DELETE | `/api/users/<user_id>` | Delete a user (can't delete self) |
| PUT | `/api/users/<user_id>/role` | Change user's global role |
| PUT | `/api/users/<user_id>/password` | Reset user's password |

### Collection Permissions (admin only)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/collections/<coll_id>/permissions` | List users with collection access |
| POST | `/api/collections/<coll_id>/permissions` | Grant user collection-level access |
| DELETE | `/api/collections/<coll_id>/permissions/<user_id>` | Revoke collection access |

### Reviewer Scopes (admin only)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/collections/<coll_id>/reviewers` | List reviewer scopes for a collection |
| POST | `/api/collections/<coll_id>/reviewers` | Create a new reviewer scope |
| DELETE | `/api/collections/<coll_id>/reviewers/<scope_id>` | Remove a reviewer scope |
| GET | `/api/users/<user_id>/reviewers` | List all reviewer scopes for a user |

### Scope Fields

| Field | Type | Description |
|---|---|---|
| `user_id` | int (required) | User ID the scope applies to |
| `match_field` | string (required) | Item field to match (e.g., `detected_language`, `subject`, `creator`) |
| `match_pattern` | string (required) | Pattern to match against the field value |
| `match_exact` | bool (optional, default: false) | If true, requires exact match; if false, uses case-insensitive substring match |
| `name` | string (optional) | Human-readable label for the scope |

## Admin Dashboard

Visit `http://localhost:5050/admin` to access the web-based admin panel.

Features:
- **Users tab**: List all users, change roles, delete users, create new users
- **Collections tab**: View collection permissions, assign/revoke access
- **Reviewer Scopes tab**: List, add, and remove reviewer scopes
- **Password change**: Change your own password

## Example Workflows

### Assign a volunteer to Sanskrit transliteration work

1. Admin logs in at `/admin`
2. Create user or use existing volunteer account
3. Grant collection access: `POST /api/collections/15/permissions` with `{user_id: 4, role: "editor"}`
4. Add reviewer scope: `POST /api/collections/15/reviewers` with `{user_id: 4, match_field: "detected_language", match_pattern: "san", match_exact: true}`
5. Volunteer logs in and can only see/edit Sanskrit items in collection 15

### Restrict a reviewer to a specific subject range

1. Create a reviewer scope with partial match:
```json
{
  "user_id": 4,
  "match_field": "subject",
  "match_pattern": "history",
  "match_exact": false
}
```

### Restrict to multiple languages

Create multiple scopes (they are OR'd together):
```json
{"user_id": 4, "match_field": "detected_language", "match_pattern": "san", "match_exact": true}
{"user_id": 4, "match_field": "detected_language", "match_pattern": "tam", "match_exact": true}
```

## Technical Details

### File Structure

```
auth_system.py          # Core auth logic: hashing, role checks, access control
app.py                  # Flask routes for auth + middleware integration
database.py             # Schema includes users, user_collection_permissions, reviewer_scopes tables
templates/auth/login.html   # Login page
templates/auth/signup.html  # Signup page
templates/admin.html        # Full-featured admin dashboard
```

### Database Schema

```sql
-- Users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,  -- bcrypt or sha256$salt$hash
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TEXT DEFAULT (datetime('now'))
);

-- Collection-level permissions (overrides global role)
CREATE TABLE user_collection_permissions (
    user_id INTEGER NOT NULL REFERENCES users(id),
    collection_id INTEGER NOT NULL REFERENCES collections(id),
    role TEXT NOT NULL DEFAULT 'viewer',
    granted_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, collection_id)
);

-- Reviewer scopes (item-level search-pattern filtering)
CREATE TABLE reviewer_scopes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id),
    collection_id INTEGER NOT NULL REFERENCES collections(id),
    name TEXT,
    match_field TEXT NOT NULL,
    match_pattern TEXT NOT NULL,
    match_exact INTEGER DEFAULT 0,
    granted_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, collection_id, match_field, match_pattern)
);
```

### Password Hashing

- Uses **bcrypt** if available (recommended)
- Falls back to **salted SHA-256** if bcrypt is not installed
- To install bcrypt: `pip install bcrypt`

### Session Management

- Flask sessions with server-side `secret_key`
- Session stores `user_id` and `user_role`
- User is loaded from DB on every request via `before_request` middleware
- Sessions expire on browser close

### Middleware Flow

1. `before_request`: Loads user from session into `flask.g`
2. `before_request` (auth_middleware): Checks all `/api/*` routes (except auth endpoints)
3. Route-level decorators: `@require_role("admin")` for admin-only routes
4. Item-level checks: `_check_item_access()` for per-item read/write access
