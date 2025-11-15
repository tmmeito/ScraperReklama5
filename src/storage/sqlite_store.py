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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT NOT NULL,
            field TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            change_type TEXT,
            changed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_listing_changes_listing ON listing_changes(listing_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_listing_changes_changed_at ON listing_changes(changed_at)"
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


def _clean_field_value(value):
    value = _normalize_value(value)
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    return value


def _serialize_change_value(value):
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return str(value)


def upsert_listing(
    conn: sqlite3.Connection,
    listing: Mapping[str, object],
    fieldnames: Sequence[str],
    *,
    timestamp: Optional[datetime] = None,
) -> None:
    upsert_many(conn, [listing], fieldnames, timestamp=timestamp)


def calculate_listing_hash(values: Mapping[str, object]) -> str:
    """Public helper that mirrors the internal hash generation logic."""

    return _calculate_listing_hash(values)


def fetch_listing_by_id(
    conn: sqlite3.Connection,
    listing_id: str,
):
    """Return the listing row for ``listing_id`` or ``None`` if missing."""

    if not listing_id:
        return None
    row = conn.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
    return dict(row) if row else None


def fetch_listings_by_ids(
    conn: sqlite3.Connection,
    ids: Iterable[object],
) -> Mapping[str, Mapping[str, object]]:
    """Fetch multiple listings at once and return a mapping of ``id`` to rows."""

    normalized_ids = [str(listing_id) for listing_id in ids if listing_id not in (None, "")]
    if not normalized_ids:
        return {}

    placeholders = ", ".join(["?"] * len(normalized_ids))
    sql = f"SELECT * FROM listings WHERE id IN ({placeholders})"
    rows = conn.execute(sql, normalized_ids).fetchall()
    return {row["id"]: dict(row) for row in rows}


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
    change_sql = (
        "INSERT INTO listing_changes (listing_id, field, old_value, new_value, change_type, changed_at)"
        " VALUES (?, ?, ?, ?, ?, ?)"
    )

    def prepare_row(item: Mapping[str, object]):
        normalized = {name: _clean_field_value(item.get(name)) for name in fieldnames}
        normalized["id"] = _ensure_listing_id(normalized)
        listing_id = normalized["id"]
        existing_row = conn.execute(
            "SELECT * FROM listings WHERE id = ?",
            (listing_id,),
        ).fetchone()
        existing = dict(existing_row) if existing_row else None

        merged = {}
        for name in fieldnames:
            value = normalized.get(name)
            if value is None and existing is not None:
                merged[name] = existing.get(name)
            else:
                merged[name] = value

        merged_hash_payload = {name: merged.get(name) for name in fieldnames}
        merged["hash"] = _calculate_listing_hash(merged_hash_payload)
        merged["created_at"] = (
            existing.get("created_at") if existing is not None else now_text
        )
        merged["last_seen"] = now_text

        changes = []
        data_changed = False
        if existing is None:
            data_changed = True
        else:
            for name in fieldnames:
                if name == "id":
                    continue
                old_value = existing.get(name)
                new_value = merged.get(name)
                if old_value != new_value:
                    data_changed = True
                    changes.append(
                        (
                            listing_id,
                            name,
                            _serialize_change_value(old_value),
                            _serialize_change_value(new_value),
                            name,
                            now_text,
                        )
                    )

        merged["updated_at"] = (
            now_text if data_changed else existing.get("updated_at")
        ) if existing is not None else now_text

        row_values = [merged.get(col) for col in columns]
        return row_values, changes

    with conn:
        for item in listings:
            row_values, changes = prepare_row(item)
            conn.execute(sql, row_values)
            if changes:
                conn.executemany(change_sql, changes)
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


def _build_filter_conditions(*, days: Optional[int] = None, search: Optional[str] = None):
    clauses = []
    params = []
    if days is not None and days > 0:
        cutoff = datetime.utcnow() - timedelta(days=days)
        clauses.append("datetime(last_seen) >= datetime(?)")
        params.append(cutoff.isoformat(timespec="seconds"))
    if search:
        pattern = f"%{search.strip().lower()}%"
        clauses.append(
            "("  #
            "LOWER(COALESCE(make, '')) LIKE ? OR "
            "LOWER(COALESCE(model, '')) LIKE ? OR "
            "LOWER(COALESCE(fuel, '')) LIKE ?"
            ")"
        )
        params.extend([pattern, pattern, pattern])
    if clauses:
        return " WHERE " + " AND ".join(clauses), params
    return "", params


def _normalized_expr(column: str) -> str:
    return f"COALESCE(NULLIF(TRIM({column}), ''), 'Unbekannt')"


def fetch_make_model_stats(
    conn: sqlite3.Connection,
    *,
    min_price: int = 0,
    days: Optional[int] = None,
    search: Optional[str] = None,
):
    """Aggregate make/model statistics using SQL directly."""

    min_price = max(0, int(min_price or 0))
    where_clause, filter_params = _build_filter_conditions(days=days, search=search)
    make_expr = _normalized_expr("make")
    model_expr = _normalized_expr("model")
    fuel_expr = _normalized_expr("fuel")
    sql = f"""
        SELECT
            {make_expr} AS make,
            {model_expr} AS model,
            {fuel_expr} AS fuel,
            COUNT(*) AS count_total,
            SUM(CASE WHEN price IS NOT NULL AND price >= ? THEN 1 ELSE 0 END) AS count_for_avg,
            SUM(CASE WHEN price IS NOT NULL AND price >= ? THEN price ELSE 0 END) AS sum_price,
            SUM(CASE WHEN price IS NOT NULL AND price < ? THEN 1 ELSE 0 END) AS excluded_low_price
        FROM listings
        {where_clause}
        GROUP BY make, model, fuel
    """
    params = [min_price, min_price, min_price] + filter_params
    rows = conn.execute(sql, params).fetchall()
    stats = {}
    for row in rows:
        key = (row["make"], row["model"], row["fuel"])
        stats[key] = {
            "count_total": int(row["count_total"]),
            "count_for_avg": int(row["count_for_avg"] or 0),
            "sum": row["sum_price"] or 0,
            "excluded_low_price": int(row["excluded_low_price"] or 0),
        }
    return stats


def fetch_model_year_stats(
    conn: sqlite3.Connection,
    *,
    min_price: int = 0,
    days: Optional[int] = None,
    search: Optional[str] = None,
):
    """Aggregate per model/year statistics using SQL directly."""

    min_price = max(0, int(min_price or 0))
    clauses = ["price IS NOT NULL", "year IS NOT NULL"]
    params = []
    if days is not None and days > 0:
        cutoff = datetime.utcnow() - timedelta(days=days)
        clauses.append("datetime(last_seen) >= datetime(?)")
        params.append(cutoff.isoformat(timespec="seconds"))
    if search:
        pattern = f"%{search.strip().lower()}%"
        clauses.append(
            "("  #
            "LOWER(COALESCE(make, '')) LIKE ? OR "
            "LOWER(COALESCE(model, '')) LIKE ? OR "
            "LOWER(COALESCE(fuel, '')) LIKE ?"
            ")"
        )
        params.extend([pattern, pattern, pattern])
    where_clause = " WHERE " + " AND ".join(clauses) if clauses else ""
    make_expr = _normalized_expr("make")
    model_expr = _normalized_expr("model")
    fuel_expr = _normalized_expr("fuel")
    sql = f"""
        SELECT
            {make_expr} AS make,
            {model_expr} AS model,
            {fuel_expr} AS fuel,
            year,
            COUNT(*) AS count_total,
            SUM(CASE WHEN price >= ? THEN 1 ELSE 0 END) AS count_for_avg,
            SUM(CASE WHEN price >= ? THEN price ELSE 0 END) AS sum_price,
            SUM(CASE WHEN price < ? THEN 1 ELSE 0 END) AS excluded_low_price
        FROM listings
        {where_clause}
        GROUP BY make, model, fuel, year
    """
    sql_params = [min_price, min_price, min_price] + params
    rows = conn.execute(sql, sql_params).fetchall()
    stats = {}
    for row in rows:
        key = (row["make"], row["model"], row["fuel"], row["year"])
        stats[key] = {
            "count_total": int(row["count_total"]),
            "count_for_avg": int(row["count_for_avg"] or 0),
            "sum": row["sum_price"] or 0,
            "excluded_low_price": int(row["excluded_low_price"] or 0),
        }
    return stats


def _parse_change_value(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        try:
            return int(value)
        except (TypeError, ValueError):
            return value


def fetch_recent_price_changes(
    conn: sqlite3.Connection,
    *,
    limit: int = 5,
):
    sql = """
        SELECT listing_id, old_value, new_value, changed_at
        FROM listing_changes
        WHERE field = 'price'
        ORDER BY datetime(changed_at) DESC
        LIMIT ?
    """
    rows = conn.execute(sql, (limit,)).fetchall()
    return [
        {
            "listing_id": row["listing_id"],
            "old_price": _parse_change_value(row["old_value"]),
            "new_price": _parse_change_value(row["new_value"]),
            "changed_at": row["changed_at"],
        }
        for row in rows
    ]

