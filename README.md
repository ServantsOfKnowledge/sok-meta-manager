# SOK MetaManager

A web-based metadata management tool for Internet Archive collections, built for Servants of Knowledge. It provides tools for syncing, editing, transliterating, and pushing metadata, as well as managing collection hierarchies and volunteer review workflows.

## Features

- **Collection Sync**: Full and smart syncs from Internet Archive collections
- **Metadata Editing**: Bulk edits, per-item changes, and change tracking
- **Transliteration Pipeline**: Supports Indic language transliteration (Bengali, Hindi, Marathi, Tamil, Telugu, Gujarati, Kannada, Malayalam, Odia, Punjabi, Sanskrit, etc.)
- **Volunteer Review Workflow**: CSV export/import for collaborative transliteration review
- **Background Jobs**: Persistent background queue for syncs, stats fetching, and bulk operations
- **FTS Search**: Full-text search across item metadata
- **Role-Based Access Control**: Admin, editor, and reviewer roles with collection-level and item-level permissions (optional)

## Quick Start

### Prerequisites

- Python 3.8+
- Internet Archive CLI:
  ```bash
  pip install internetarchive
  ```

### Installation

```bash
pip install -r requirements.txt
python3 -c "import database; database.init_db()"
python3 app.py
```

Then open `http://localhost:5050`

### Default Admin Credentials

- Username: `admin`
- Password: `admin`

## Documentation

- [Authentication & Authorization](docs/AUTH.md) — User roles, collection permissions, reviewer scopes

## Architecture

- **Catalog DB**: Stores collection metadata, job queue, sub-collections
- **Per-Collection DBs**: Sharded SQLite databases for each collection's items
- **Flask App**: REST API + single-page frontend
- **Background Workers**: Parallel job processing with cancellation support

## License

MIT License
