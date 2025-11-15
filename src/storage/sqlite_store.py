"""Utility helpers for persisting scraper results in SQLite."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Iterable, Mapping, Optional, Sequence

DEFAULT_DB_PATH = os.path.join("data", "reklama5.db")


def open_database(db_path: str) -> sqlite3.Connection:
    """Open (and create if needed) a SQLite database at ``db_path``."""
    normalized = os.path.abspath(db_path)
    directory = os.path.dirname(normalized)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(normalized)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection, fieldnames: Sequence[str]) -> None:
    """Ensure that the ``listings`` table and indexes exist."""
    column_defs = []
    for name in fieldnames:
        if name == "id":
            column_defs.append('"id" TEXT NOT NULL PRIMARY KEY')
        elif name in {"price", "year", "km", "kw", "ps"}:
            column_defs.append(f'"{name}" INTEGER')
        elif name == "promoted":
            column_defs.append(f'"{name}" INTEGER DEFAULT 0')
        else:
            column_defs.append(f'"{name}" TEXT')
    column_defs.append('"hash" TEXT')
    column_defs.append('"created_at" TEXT NOT NULL')
    column_defs.append('"updated_at" TEXT NOT NULL')
    column_defs.append('"last_seen" TEXT NOT NULL')

    schema_sql = (
        "CREATE TABLE IF NOT EXISTS listings (\n        "
        + ",\n        ".join(column_defs)
        + "\n    )"
    )
    conn.execute(schema_sql)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings(last_seen)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_listings_hash ON listings(hash)"
    )
    conn.commit()


def _normalize_value(value):
    if isinstance(value, bool):
        return int(value)
    return value


def _ensure_listing_id(values: Mapping[str, object]) -> str:
    listing_id = values.get("id")
    if listing_id is None or listing_id == "":
        listing_id = _calculate_listing_hash(values)
    return str(listing_id)


def _calculate_listing_hash(values: Mapping[str, object]) -> str:
    serializable = {k: values.get(k) for k in sorted(values.keys())}
    payload = json.dumps(serializable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def upsert_listing(
    conn: sqlite3.Connection,
    listing: Mapping[str, object],
    fieldnames: Sequence[str],
    *,
    timestamp: Optional[datetime] = None,
) -> None:
    upsert_many(conn, [listing], fieldnames, timestamp=timestamp)


def upsert_many(
    conn: sqlite3.Connection,
    listings: Iterable[Mapping[str, object]],
    fieldnames: Sequence[str],
    *,
    timestamp: Optional[datetime] = None,
) -> int:
    """Insert or update ``listings`` using ``fieldnames`` order."""
    listings = list(listings)
    if not listings:
        return 0

    now = timestamp or datetime.utcnow()
    now_text = now.isoformat(timespec="seconds")
    columns = list(fieldnames) + ["hash", "created_at", "updated_at", "last_seen"]
    placeholders = ", ".join(["?"] * len(columns))
    update_assignments = ", ".join(
        f'{col}=excluded.{col}'
        for col in columns
        if col not in {"id", "created_at"}
    )
    sql = (
        f"INSERT INTO listings ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {update_assignments}"
    )

    def row_values(item: Mapping[str, object]):
        normalized = {name: _normalize_value(item.get(name)) for name in fieldnames}
        normalized["id"] = _ensure_listing_id(normalized)
        normalized["hash"] = _calculate_listing_hash(normalized)
        normalized["created_at"] = now_text
        normalized["updated_at"] = now_text
        normalized["last_seen"] = now_text
        return [normalized.get(col) for col in columns]

    with conn:
        for item in listings:
            conn.execute(sql, row_values(item))
    return len(listings)


def fetch_recent_listings(
    conn: sqlite3.Connection,
    *,
    limit: Optional[int] = 100,
    days: Optional[int] = None,
):
    query = "SELECT * FROM listings"
    params = []
    if days is not None and days > 0:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query += " WHERE datetime(last_seen) >= datetime(?)"
        params.append(cutoff.isoformat(timespec="seconds"))
    query += " ORDER BY datetime(last_seen) DESC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    cursor = conn.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]


def count_listings(conn: sqlite3.Connection) -> int:
    cursor = conn.execute("SELECT COUNT(*) FROM listings")
    row = cursor.fetchone()
    return int(row[0]) if row else 0

