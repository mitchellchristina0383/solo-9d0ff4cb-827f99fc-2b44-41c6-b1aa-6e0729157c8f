"""SQLite storage and online FTS index management.

The important invariant is that every generation of the search index is a
separate FTS5 table.  A generation is only switched live after it has indexed
every row in ``logs``.  While a rebuild is running, new log rows are written to
both the currently active index and the index being built, so activation does
not need a long catch-up window.

Writes and activation are serialized with ``BEGIN IMMEDIATE`` (and an
in-process lock).  Readers pick one generation id and only ever query that
generation's FTS table; retired tables are kept, so an in-flight search cannot
observe a half-switched index.
"""

from __future__ import annotations

import os
import re
import secrets
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

# A run is either a Latin/digit word or a run of non-ASCII letters (CJK
# and other scripts).  Punctuation separates tokens.
_RUN_RE = re.compile(r"[0-9A-Za-z_]+|[^\W\d_]+")
_SAFE_TABLE_RE = re.compile(r"^fts_[0-9a-f]{16}$")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    service     TEXT NOT NULL,
    body        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_logs_service_id
    ON logs(service, id);

CREATE TABLE IF NOT EXISTS index_generations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    status          TEXT NOT NULL
                    CHECK (status IN ('building', 'active', 'failed', 'retired')),
    created_at      TEXT NOT NULL,
    activated_at    TEXT,
    finished_at     TEXT,
    start_id        INTEGER NOT NULL DEFAULT 0,
    build_cursor    INTEGER NOT NULL DEFAULT 0,
    indexed_through INTEGER NOT NULL DEFAULT 0,
    doc_count       INTEGER NOT NULL DEFAULT 0,
    fts_table       TEXT NOT NULL,
    error           TEXT
);

-- At most one active index and at most one in-progress rebuild.
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_active_generation
    ON index_generations(status) WHERE status = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_building_generation
    ON index_generations(status) WHERE status = 'building';
"""


class RebuildInProgress(RuntimeError):
    """Raised when a rebuild is already running."""


class IndexUnavailable(RuntimeError):
    """Raised when no active index exists."""


class BuildAborted(RuntimeError):
    """Raised internally when a build is no longer the current build."""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _runs(text: str) -> Iterable[str]:
    for match in _RUN_RE.finditer(text):
        yield match.group(0)


def document_tokens(text: str) -> list[str]:
    """Tokens stored in the FTS table.

    Latin/digit runs are stored as whole, case-folded words.  CJK runs are
    represented by their characters *and* adjacent character pairs; the
    characters make a one-character query work, while the pairs make normal
    Chinese words and phrases work without a language-specific tokenizer.
    """
    tokens: list[str] = []
    for run in _runs(text):
        if run[0].isascii():
            tokens.append(run.casefold())
            continue
        if len(run) == 1:
            tokens.append(run)
        else:
            tokens.extend(run)
            tokens.extend(run[i : i + 2] for i in range(len(run) - 1))
    # Duplicate tokens only add positions we do not need.
    return list(dict.fromkeys(tokens))


def query_tokens(text: str) -> list[tuple[str, bool]]:
    """(token, is_prefix) pairs AND-ed together for a search query.

    Latin tokens are matched by prefix so that a query for ``timeout`` finds
    ``timeouts`` and ``timeout=30s``; log greps are commonly partial words.
    CJK words are matched exactly through their character pairs, which also
    keeps single-character queries working.
    """
    tokens: list[tuple[str, bool]] = []
    for run in _runs(text):
        if run[0].isascii():
            tokens.append((run.casefold(), True))
        elif len(run) == 1:
            tokens.append((run, False))
        else:
            tokens.extend((run[i : i + 2], False) for i in range(len(run) - 1))
    return list(dict.fromkeys(tokens))


def fts_match_expression(text: str) -> str | None:
    tokens = query_tokens(text)
    if not tokens:
        return None
    # Tokens only contain alphanumerics and CJK characters.
    parts = [
        f'"{token}"*' if prefix else f'"{token}"' for token, prefix in tokens
    ]
    return " AND ".join(parts)


class IndexManager:
    """Owns SQLite connections, writes and the rebuild worker."""

    def __init__(self, db_path: str | os.PathLike[str], *, chunk_size: int = 500) -> None:
        self.db_path = str(db_path)
        self.chunk_size = max(1, chunk_size)
        self._write_lock = threading.RLock()
        self._init_lock = threading.Lock()
        self._initialized = False
        self._shutdown = threading.Event()
        self._build_thread: threading.Thread | None = None
        # Test/observability hook: called with (event, payload).  Exceptions
        # raised by the hook fail the build, which is useful in tests.
        self.build_hook: Callable[[str, dict[str, Any]], None] | None = None
        # Test hook called after a search has picked its generation.
        self.search_hook: Callable[[dict[str, Any]], None] | None = None

    # ------------------------------------------------------------------ setup
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _write_tx(self):
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def _init_database(self) -> None:
        Path(self.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            try:
                conn.executescript(_SCHEMA)
            except sqlite3.OperationalError as exc:
                if "fts5" in str(exc).lower():
                    raise RuntimeError(
                        "此 SQLite 未启用 FTS5 模块，无法建立检索索引"
                    ) from exc
                raise
        finally:
            conn.close()

    def initialize(self) -> None:
        """Prepare the DB, recover interrupted builds and ensure an active index."""
        with self._init_lock:
            if self._initialized:
                return
            self._init_database()
            self._recover_interrupted_builds()
            self._drop_orphan_tables()

            if self._get_active() is None:
                generation = self._create_generation(require_active=False)
                # Initial build is synchronous: the HTTP server is not serving
                # yet, so there is nothing to keep available while it runs.
                self._run_build(generation["id"])
                active = self._get_active()
                if active is None:
                    raise RuntimeError("initial search index could not be built")
            self._initialized = True

    # --------------------------------------------------------------- helpers
    def _get_generation(self, generation_id: int) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM index_generations WHERE id = ?", (generation_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def _get_active(self) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM index_generations WHERE status = 'active'"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def _notify(self, event: str, payload: dict[str, Any]) -> None:
        hook = self.build_hook
        if hook is not None:
            hook(event, payload)

    def _drop_table(self, table: str) -> None:
        if not _SAFE_TABLE_RE.match(table):
            return
        try:
            with self._write_tx() as conn:
                conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        except sqlite3.Error:
            # It will be removed by orphan cleanup on the next startup.
            pass

    def _recover_interrupted_builds(self) -> None:
        stale: list[dict[str, Any]] = []
        with self._write_tx() as conn:
            rows = conn.execute(
                "SELECT id, fts_table FROM index_generations WHERE status = 'building'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE index_generations "
                    "SET status = 'failed', finished_at = ?, "
                    "    error = ? "
                    "WHERE id = ? AND status = 'building'",
                    (
                        utcnow(),
                        "进程在重建完成前重启，半成品索引已丢弃",
                        row["id"],
                    ),
                )
                stale.append(dict(row))
        for row in stale:
            self._drop_table(row["fts_table"])

    def _drop_orphan_tables(self) -> None:
        """Drop FTS tables that cannot be in use after a restart.

        Only the active generation is needed: an interrupted build has just
        been marked failed, and retired generations were only kept so that
        in-flight searches of the previous process could finish.  Dropping
        the rest also cleans up a failed build whose table could not be
        removed immediately (e.g. the DB was busy).
        """
        conn = self._connect()
        try:
            keep = {
                row["fts_table"]
                for row in conn.execute(
                    "SELECT fts_table FROM index_generations WHERE status = 'active'"
                ).fetchall()
            }
            tables = [
                row["name"]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name LIKE 'fts_%'"
                ).fetchall()
            ]
        finally:
            conn.close()
        for table in tables:
            if table not in keep and _SAFE_TABLE_RE.match(table):
                self._drop_table(table)

    # --------------------------------------------------------------- rebuild
    def _create_generation(self, *, require_active: bool) -> dict[str, Any]:
        table = f"fts_{secrets.token_hex(8)}"
        with self._write_tx() as conn:
            if conn.execute(
                "SELECT 1 FROM index_generations WHERE status = 'building'"
            ).fetchone():
                raise RebuildInProgress("已有重建任务正在进行")
            if require_active and not conn.execute(
                "SELECT 1 FROM index_generations WHERE status = 'active'"
            ).fetchone():
                raise IndexUnavailable("当前没有可用的索引")
            start_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM logs"
            ).fetchone()[0]
            # DDL is transactional in SQLite: a crash before COMMIT leaves no
            # partial table behind.
            conn.execute(
                f'CREATE VIRTUAL TABLE "{table}" '
                "USING fts5(tokens, tokenize = 'unicode61')"
            )
            cursor = conn.execute(
                "INSERT INTO index_generations "
                "(status, created_at, start_id, build_cursor, indexed_through, "
                " doc_count, fts_table) "
                "VALUES ('building', ?, ?, 0, 0, 0, ?)",
                (utcnow(), start_id, table),
            )
            generation_id = int(cursor.lastrowid)
        generation = self._get_generation(generation_id)
        assert generation is not None
        return generation

    def start_rebuild(self) -> dict[str, Any]:
        """Create a generation and start building it in the background."""
        generation = self._create_generation(require_active=True)
        thread = threading.Thread(
            target=self._run_build,
            args=(generation["id"],),
            name=f"index-build-{generation['id']}",
            daemon=True,
        )
        self._build_thread = thread
        thread.start()
        return generation

    def _run_build(self, generation_id: int) -> None:
        try:
            while not self._shutdown.is_set():
                generation = self._get_generation(generation_id)
                if generation is None or generation["status"] != "building":
                    return
                # Only the snapshot [0, start_id] is fetched here.  Rows
                # committed after the generation was created are written into
                # this index directly by add_log, so the build never inserts
                # the same rowid twice.
                rows = self._fetch_batch(
                    generation["build_cursor"], generation["start_id"]
                )
                if not rows:
                    break
                self._index_batch(generation_id, rows)
                after = self._get_generation(generation_id)
                if after is None or after["status"] != "building":
                    return
                self._notify(
                    "batch",
                    {
                        "generation_id": generation_id,
                        "rows": len(rows),
                        "build_cursor": after["build_cursor"],
                        "start_id": after["start_id"],
                    },
                )

            if self._shutdown.is_set():
                raise BuildAborted("进程正在停止，重建已中止")
            self._notify("before_activate", {"generation_id": generation_id})
            self._activate(generation_id)
            self._notify("activated", {"generation_id": generation_id})
        except BaseException as exc:  # a failed build must never hurt the old index
            self._fail_generation(generation_id, exc)
            self._notify(
                "failed",
                {"generation_id": generation_id, "error": str(exc)},
            )

    def _fetch_batch(self, cursor: int, start_id: int) -> list[dict[str, Any]]:
        """Rows from the generation's initial snapshot not indexed yet."""
        if cursor >= start_id:
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT id, body FROM logs "
                "WHERE id > ? AND id <= ? ORDER BY id LIMIT ?",
                (cursor, start_id, self.chunk_size),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def _index_batch(self, generation_id: int, rows: list[dict[str, Any]]) -> None:
        tokens_by_id = [
            (row["id"], " ".join(document_tokens(row["body"]))) for row in rows
        ]
        with self._write_tx() as conn:
            generation = conn.execute(
                "SELECT status, fts_table, build_cursor FROM index_generations "
                "WHERE id = ?",
                (generation_id,),
            ).fetchone()
            if generation is None or generation["status"] != "building":
                raise BuildAborted("该重建已不是有效任务")
            cursor = generation["build_cursor"]
            pending = [(log_id, tokens) for log_id, tokens in tokens_by_id if log_id > cursor]
            if not pending:
                return
            conn.executemany(
                f'INSERT INTO "{generation["fts_table"]}"(rowid, tokens) VALUES (?, ?)',
                pending,
            )
            last_id = pending[-1][0]
            conn.execute(
                "UPDATE index_generations "
                "SET build_cursor = ?, "
                "    indexed_through = MAX(indexed_through, ?), "
                "    doc_count = doc_count + ? "
                "WHERE id = ? AND status = 'building'",
                (last_id, last_id, len(pending), generation_id),
            )

    def _activate(self, generation_id: int) -> None:
        with self._write_tx() as conn:
            generation = conn.execute(
                "SELECT * FROM index_generations WHERE id = ?",
                (generation_id,),
            ).fetchone()
            if generation is None or generation["status"] != "building":
                raise BuildAborted("该重建已不是有效任务")

            table = generation["fts_table"]
            total_logs = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
            indexed = conn.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            if indexed != total_logs:
                raise RuntimeError(
                    f"新索引不完整（已索引 {indexed} 条，日志共 {total_logs} 条），拒绝启用"
                )
            max_log_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM logs"
            ).fetchone()[0]

            conn.execute(
                "UPDATE index_generations SET status = 'retired' "
                "WHERE status = 'active'"
            )
            conn.execute(
                "UPDATE index_generations "
                "SET status = 'active', activated_at = ?, finished_at = ?, "
                "    indexed_through = ?, doc_count = ? "
                "WHERE id = ? AND status = 'building'",
                (utcnow(), utcnow(), max_log_id, total_logs, generation_id),
            )

    def _fail_generation(self, generation_id: int, exc: BaseException) -> None:
        table: str | None = None
        try:
            with self._write_tx() as conn:
                generation = conn.execute(
                    "SELECT status, fts_table FROM index_generations WHERE id = ?",
                    (generation_id,),
                ).fetchone()
                if generation is not None and generation["status"] == "building":
                    conn.execute(
                        "UPDATE index_generations "
                        "SET status = 'failed', finished_at = ?, error = ? "
                        "WHERE id = ? AND status = 'building'",
                        (utcnow(), str(exc)[:2000], generation_id),
                    )
                    table = generation["fts_table"]
        except sqlite3.Error:
            return
        if table:
            self._drop_table(table)

    # ---------------------------------------------------------------- writes
    def add_log(self, service: str, body: str) -> dict[str, Any]:
        """Insert a log and index it in both active and building generations."""
        tokens = " ".join(document_tokens(body))
        created_at = utcnow()
        with self._write_tx() as conn:
            cursor = conn.execute(
                "INSERT INTO logs(service, body, created_at) VALUES (?, ?, ?)",
                (service, body, created_at),
            )
            log_id = int(cursor.lastrowid)
            generations = conn.execute(
                "SELECT id, fts_table FROM index_generations "
                "WHERE status IN ('active', 'building')"
            ).fetchall()
            for generation in generations:
                conn.execute(
                    f'INSERT INTO "{generation["fts_table"]}"(rowid, tokens) '
                    "VALUES (?, ?)",
                    (log_id, tokens),
                )
                conn.execute(
                    "UPDATE index_generations "
                    "SET doc_count = doc_count + 1, "
                    "    indexed_through = MAX(indexed_through, ?) "
                    "WHERE id = ?",
                    (log_id, generation["id"]),
                )
        return {
            "id": log_id,
            "service": service,
            "body": body,
            "created_at": created_at,
        }

    # ---------------------------------------------------------------- search
    def search(
        self,
        query: str | None,
        service: str | None,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        query = (query or "").strip()
        service = (service or "").strip()
        if not query and not service:
            raise ValueError("请至少提供服务名或关键词")

        conn = self._connect()
        try:
            # Pin one read snapshot.  The generation id and the FTS query are
            # therefore from the same point in time even if a rebuild happens
            # to activate concurrently.
            conn.execute("BEGIN")
            try:
                return self._search_in_snapshot(conn, query, service, limit, offset)
            finally:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def _search_in_snapshot(
        self,
        conn: sqlite3.Connection,
        query: str,
        service: str,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        active = conn.execute(
            "SELECT * FROM index_generations WHERE status = 'active'"
        ).fetchone()
        if active is None:
            raise IndexUnavailable("当前没有可用的索引")
        active = dict(active)
        hook = self.search_hook
        if hook is not None:
            hook(active)

        if query:
            match = fts_match_expression(query)
            if match is None:
                return {
                    "generation": active,
                    "mode": "fts",
                    "total": 0,
                    "items": [],
                }
            table = active["fts_table"]
            # FTS5 MATCH requires the real table name; aliases are not
            # accepted in the MATCH expression.
            where = (
                f'FROM "{table}" JOIN logs l ON l.id = "{table}".rowid '
                f'WHERE "{table}" MATCH ?'
            )
            params: list[Any] = [match]
            if service:
                where += " AND l.service = ?"
                params.append(service)
            total = conn.execute(f"SELECT COUNT(*) {where}", params).fetchone()[0]
            rows = conn.execute(
                f"SELECT l.id, l.service, l.body, l.created_at "
                f"{where} ORDER BY l.id DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            mode = "fts"
        else:
            # Service-only listing still goes through the active index table,
            # so the result set is exactly the indexed documents of that
            # generation (never a mix of generations).
            table = active["fts_table"]
            total = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" JOIN logs l '
                f'ON l.id = "{table}".rowid WHERE l.service = ?',
                (service,),
            ).fetchone()[0]
            rows = conn.execute(
                f'SELECT l.id, l.service, l.body, l.created_at '
                f'FROM "{table}" JOIN logs l ON l.id = "{table}".rowid '
                f"WHERE l.service = ? ORDER BY l.id DESC LIMIT ? OFFSET ?",
                (service, limit, offset),
            ).fetchall()
            mode = "service"

        return {
            "generation": active,
            "mode": mode,
            "total": int(total),
            "items": [dict(row) for row in rows],
        }

    # ---------------------------------------------------------------- status
    def status(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            active = conn.execute(
                "SELECT * FROM index_generations WHERE status = 'active'"
            ).fetchone()
            building = conn.execute(
                "SELECT * FROM index_generations WHERE status = 'building'"
            ).fetchone()
            latest = conn.execute(
                "SELECT * FROM index_generations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            total_logs = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
        finally:
            conn.close()

        building_dict = dict(building) if building else None
        progress = None
        if building_dict is not None:
            start_id = building_dict["start_id"]
            cursor = building_dict["build_cursor"]
            percent = 100.0 if start_id <= 0 else min(100.0, cursor * 100.0 / start_id)
            progress = {
                # Rows committed after the rebuild started are indexed by
                # add_log and do not count toward the snapshot progress.
                "snapshot_total": start_id,
                "build_cursor": cursor,
                "percent": round(percent, 1),
            }
        return {
            "active": dict(active) if active else None,
            "building": building_dict,
            "latest": dict(latest) if latest else None,
            "progress": progress,
            "total_logs": int(total_logs),
        }

    def services(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT service, COUNT(*) AS count FROM logs "
                "GROUP BY service ORDER BY service COLLATE NOCASE"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def health(self) -> bool:
        conn = self._connect()
        try:
            conn.execute("SELECT 1").fetchone()
            return (
                conn.execute(
                    "SELECT 1 FROM index_generations WHERE status = 'active'"
                ).fetchone()
                is not None
            )
        except sqlite3.Error:
            return False
        finally:
            conn.close()

    def shutdown(self) -> None:
        self._shutdown.set()
        thread = self._build_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
