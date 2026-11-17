"""Tests for the online index rebuild behaviour."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import re
import sqlite3
import threading
import time

import pytest

from app.indexer import (
    IndexManager,
    LogNotFound,
    RebuildInProgress,
    document_tokens,
    fts_match_expression,
)


@pytest.fixture()
def manager(tmp_path):
    idx = IndexManager(str(tmp_path / "logs.db"))
    idx.initialize()
    yield idx
    idx.shutdown()


def wait_for(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError("condition was not met before timeout")


def active_id(idx):
    status = idx.status()
    return status["active"]["id"] if status["active"] else None


def building(idx):
    return idx.status()["building"]


# --------------------------------------------------------------- tokenizer
def test_tokens_cover_cjk_words_and_latin_words():
    tokens = document_tokens("用户登录失败 error 500")
    assert "登录" in tokens
    assert "用户" in tokens
    assert "error" in tokens
    assert "500" in tokens
    assert fts_match_expression("登录失败") == '"登录" AND "录失" AND "失败"'


def test_tokens_cover_other_scripts():
    tokens = document_tokens("日本語のエラー")
    assert {"日", "本", "語", "の", "エ", "ラ", "ー"} <= set(tokens)
    assert "エラー" not in tokens  # bigram index: no single token that long
    assert "エラ" in tokens


def test_empty_query_produces_no_match_expression():
    assert fts_match_expression("   ") is None
    assert fts_match_expression("!!!") is None


# ------------------------------------------------------------ basic search
def test_search_is_case_insensitive_for_latin(manager):
    manager.add_log("api", "Upstream TIMEOUT after 30s")
    assert manager.search("timeout", None)["total"] == 1
    assert manager.search("UPSTREAM", None)["total"] == 1


def test_search_matches_chinese_word_and_substring(manager):
    manager.add_log("auth", "用户登录失败：密码错误")
    assert manager.search("登录", None)["total"] == 1
    assert manager.search("登录失败", None)["total"] == 1
    assert manager.search("密码", None)["total"] == 1
    assert manager.search("退出", None)["total"] == 0


def test_service_only_search_uses_active_index(manager):
    manager.add_log("auth", "x")
    manager.add_log("api", "y")
    result = manager.search(None, "auth")
    assert result["total"] == 1
    assert result["items"][0]["service"] == "auth"


# ---------------------------------------------------------- rebuild basics
def test_rebuild_copies_existing_logs(manager):
    for i in range(25):
        manager.add_log("svc", f"事件 {i} 已提交")
    old = active_id(manager)
    manager.start_rebuild()
    wait_for(lambda: active_id(manager) != old)
    assert manager.search("已提交", None)["total"] == 25


def test_only_one_rebuild_at_a_time(manager):
    manager.build_hook = lambda event, payload: time.sleep(0.01)
    manager.start_rebuild()
    with pytest.raises(RebuildInProgress):
        manager.start_rebuild()
    wait_for(lambda: building(manager) is None)


def test_search_actually_uses_the_index(manager):
    """A row removed from the FTS table must disappear from keyword search."""
    manager.add_log("svc", "索引内的关键词 zqx")
    table = manager.status()["active"]["fts_table"]
    with manager._write_tx() as conn:
        conn.execute(f'DELETE FROM "{table}"')
    assert manager.search("zqx", None)["total"] == 0


# ------------------------------------------------ write/rebuild interleaving
def test_logs_written_during_rebuild_are_in_new_index(manager):
    old = active_id(manager)
    for i in range(40):
        manager.add_log("svc", f"重建前日志 {i} premark")

    seen_batches = []
    lock = threading.Lock()

    def hook(event, payload):
        if event == "batch":
            with lock:
                seen_batches.append(payload)
            # Interleave writes while the builder works.
            manager.add_log("svc", f"重建中日志 {len(seen_batches)} midmark")

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: len(seen_batches) >= 1)
    # And a write from the request thread while the build continues.
    manager.add_log("svc", "线程写入 midmark")
    wait_for(lambda: active_id(manager) != old)

    assert len(seen_batches) >= 1
    # Every previously submitted log is searchable in the new index.
    pre = manager.search("premark", None)
    mid = manager.search("midmark", None)
    all_logs = manager.search(None, "svc")
    assert pre["total"] == 40
    assert mid["total"] == len(seen_batches) + 1
    assert all_logs["total"] == 40 + mid["total"]
    assert all_logs["generation"]["id"] == active_id(manager)


def test_new_index_includes_rows_committed_right_before_activation(manager):
    manager.add_log("svc", "before")
    activation_seen = threading.Event()

    def hook(event, payload):
        if event == "before_activate":
            # A request thread commits one more log just before activation.
            manager.add_log("svc", "latewrite")
            activation_seen.set()

    manager.build_hook = hook
    old = active_id(manager)
    manager.start_rebuild()
    wait_for(lambda: active_id(manager) != old)
    assert activation_seen.is_set()
    assert manager.search("latewrite", None)["total"] == 1


def test_concurrent_writes_do_not_duplicate_or_drop_results(manager):
    old = active_id(manager)
    for i in range(20):
        manager.add_log("svc", f"基础日志 base{i}")
    manager.start_rebuild()
    stop = threading.Event()
    written = []

    def writer(worker):
        n = 0
        while not stop.is_set() and n < 30:
            entry = manager.add_log("svc", f"并发日志 worker{worker} n{n}")
            written.append(entry["id"])
            n += 1

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stop.set()
    wait_for(lambda: active_id(manager) != old)

    result = manager.search("worker", None, limit=200)
    ids = [item["id"] for item in result["items"]]
    assert len(ids) == len(set(ids))
    assert set(ids) == set(written)
    assert result["total"] == len(written)

    every = manager.search(None, "svc", limit=200)
    ids = [item["id"] for item in every["items"]]
    assert len(ids) == len(set(ids))
    assert every["total"] == 20 + len(written)


# -------------------------------------------------- failure / crash safety
def test_failed_build_keeps_old_index_and_can_be_retried(manager):
    manager.add_log("svc", "旧索引仍然可查 oldmark")
    old = active_id(manager)

    def hook(event, payload):
        if event == "before_activate":
            raise RuntimeError("模拟重建失败")

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: manager.status()["latest"]["status"] == "failed")

    assert active_id(manager) == old
    assert manager.search("oldmark", None)["total"] == 1
    status = manager.status()
    assert status["building"] is None
    assert "模拟重建失败" in status["latest"]["error"]
    # The half-built table is gone.
    with sqlite3.connect(manager.db_path) as conn:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fts_%'"
            )
            if re.fullmatch(r"fts_[0-9a-f]{16}", row[0])
        ]
    assert len(tables) == 1

    # Retry succeeds and serves the same data.
    manager.build_hook = None
    manager.start_rebuild()
    wait_for(lambda: active_id(manager) != old)
    assert manager.search("oldmark", None)["total"] == 1


def test_failed_new_index_is_never_activated_even_with_full_doc_count(manager):
    """A build that indexed every row but failed must not go live."""
    for i in range(5):
        manager.add_log("svc", f"半成品 half{i}")
    old = active_id(manager)
    manager.build_hook = lambda event, payload: (
        (_ for _ in ()).throw(RuntimeError("激活前崩溃"))
        if event == "before_activate"
        else None
    )
    manager.start_rebuild()
    wait_for(lambda: manager.status()["latest"]["status"] == "failed")
    assert active_id(manager) == old
    assert manager.search("half", None)["total"] == 5


def test_failure_during_batching_keeps_old_index(manager):
    manager.add_log("svc", "可查 keepmark")
    old = active_id(manager)

    def hook(event, payload):
        if event == "batch":
            raise RuntimeError("批次中断")

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: manager.status()["latest"]["status"] == "failed")
    assert active_id(manager) == old
    assert manager.search("keepmark", None)["total"] == 1


def test_restart_discards_half_built_index(manager):
    for i in range(30):
        manager.add_log("svc", f"日志 restart{i}")

    # Freeze the builder at the first batch so the build stays incomplete.
    freeze = threading.Event()

    def hook(event, payload):
        if event == "batch":
            freeze.wait(5)

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: manager.status()["building"] is not None)
    wait_for(lambda: manager.status()["building"]["build_cursor"] > 0)
    old = active_id(manager)
    db_path = manager.db_path
    # Simulate an abrupt process restart: no shutdown of the old manager.
    frozen = IndexManager(db_path)
    frozen.build_hook = None
    frozen.initialize()
    try:
        status = frozen.status()
        assert status["building"] is None
        assert status["active"]["id"] == old
        assert status["latest"]["status"] == "failed"
        assert frozen.search("restart1", None)["total"] >= 10
        # A new rebuild can be started after recovery.
        old_frozen = active_id(frozen)
        frozen.start_rebuild()
        wait_for(lambda: active_id(frozen) != old_frozen)
        assert frozen.search("restart", None)["total"] == 30
    finally:
        frozen.shutdown()
        freeze.set()


def test_restart_mid_build_without_active_index_is_recovered(tmp_path):
    db = str(tmp_path / "crash.db")
    idx = IndexManager(db)
    idx.initialize()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE index_generations SET status = 'failed' WHERE status = 'active'"
        )
        conn.execute(
            "INSERT INTO index_generations (status, created_at, fts_table) "
            "VALUES ('building', 'x', 'fts_0123456789abcdef')"
        )
        conn.execute('CREATE VIRTUAL TABLE "fts_0123456789abcdef" USING fts5(tokens)')
    recovered = IndexManager(db)
    recovered.initialize()
    try:
        status = recovered.status()
        assert status["building"] is None
        assert status["active"] is not None
        assert status["active"]["status"] == "active"
        with sqlite3.connect(db) as conn:
            table = conn.execute(
                "SELECT fts_table FROM index_generations WHERE id = ?",
                (status["active"]["id"],),
            ).fetchone()[0]
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name = ?", (table,)
            ).fetchone()
            assert exists
    finally:
        recovered.shutdown()


# -------------------------------------------- reads during an active build
def test_search_during_rebuild_never_mixes_generations(manager):
    for i in range(50):
        manager.add_log("svc", f"混合检查 mix{i}")

    old_index = manager.status()["active"]
    snapshots = []

    def hook(event, payload):
        if event == "batch":
            result = manager.search("mix", None)
            snapshots.append(result["generation"]["id"])
            manager.add_log("svc", "混合检查 mixnew")

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: building(manager) is None)

    assert snapshots, "expected at least one search during the rebuild"
    # Reads during the build used the old generation and were complete.
    assert all(snapshot == old_index["id"] for snapshot in snapshots)
    assert manager.search("mix", None)["total"] == 50 + len(snapshots)
    assert active_id(manager) != old_index["id"]


def test_inflight_search_keeps_its_generation_across_activation(manager):
    manager.add_log("svc", "切换前 beforemark")
    old_index = active_id(manager)
    during = threading.Event()
    resume = threading.Event()
    observed = {}

    def search_hook(active):
        if active["id"] == old_index and not during.is_set():
            during.set()
            # Pause this search after it pinned the old generation; a rebuild
            # activates a new one in the meantime.
            resume.wait(5)

    manager.search_hook = search_hook
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(manager.search, "beforemark", None)
        assert during.wait(5)
        manager.start_rebuild()
        wait_for(lambda: active_id(manager) != old_index)
        manager.add_log("svc", "切换后 aftermark")
        resume.set()
        observed = pending.result(timeout=10)

    assert observed["generation"]["id"] == old_index
    assert observed["total"] == 1
    assert observed["items"][0]["body"] == "切换前 beforemark"
    # The new index has both logs.
    assert manager.search("before", None)["total"] == 1
    assert manager.search("after", None)["total"] == 1


def test_retired_indexes_are_not_returned(manager):
    manager.add_log("svc", "一次性 onlyonce")
    first = active_id(manager)
    second = None
    for _ in range(2):
        old = active_id(manager)
        manager.start_rebuild()
        wait_for(lambda: building(manager) is None)
        assert active_id(manager) != old
        if second is None:
            second = active_id(manager)

    conn = sqlite3.connect(manager.db_path)
    try:
        rows = conn.execute(
            "SELECT id, status FROM index_generations ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    statuses = {row[0]: row[1] for row in rows}
    assert statuses[first] == "retired"
    assert statuses[second] == "retired"
    # Exactly one active generation at a time.
    assert sum(1 for status in statuses.values() if status == "active") == 1
    assert manager.search("onlyonce", None)["total"] == 1


def test_rebuild_progress_is_monotonic(manager):
    for i in range(120):
        manager.add_log("svc", f"进度 {i}")
    manager.chunk_size = 10
    cursors = []

    def hook(event, payload):
        if event == "batch":
            cursors.append(payload["build_cursor"])

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: building(manager) is None)
    assert cursors == sorted(cursors)
    assert len(cursors) >= 2


# ------------------------------------------------------------- retraction
def test_retracted_log_disappears_from_search_and_services(manager):
    keep = manager.add_log("auth", "登录成功 okmark")
    gone = manager.add_log("auth", "误录日志 badmark")
    assert manager.search("badmark", None)["total"] == 1
    assert manager.services() == [{"service": "auth", "count": 2}]

    retracted = manager.retract_log(gone["id"])
    assert retracted["id"] == gone["id"]
    assert retracted["already_retracted"] is False
    assert retracted["deleted_at"]

    assert manager.search("badmark", None)["total"] == 0
    assert manager.search("okmark", None)["total"] == 1
    by_service = manager.search(None, "auth")
    assert by_service["total"] == 1
    assert by_service["items"][0]["id"] == keep["id"]
    assert manager.services() == [{"service": "auth", "count": 1}]
    status = manager.status()
    assert status["total_logs"] == 1
    assert status["active"]["doc_count"] == 2  # FTS row kept for the invariant

    # The log row itself is preserved, only marked.
    conn = sqlite3.connect(manager.db_path)
    try:
        row = conn.execute(
            "SELECT body, deleted_at FROM logs WHERE id = ?", (gone["id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row[0] == "误录日志 badmark"
    assert row[1] is not None


def test_retraction_is_idempotent(manager):
    log = manager.add_log("svc", "重复撤回 idem")
    first = manager.retract_log(log["id"])
    second = manager.retract_log(log["id"])
    assert second["already_retracted"] is True
    assert second["deleted_at"] == first["deleted_at"]
    assert manager.search("idem", None)["total"] == 0
    assert manager.search(None, "svc")["total"] == 0
    assert manager.services() == []
    assert manager.status()["total_logs"] == 0


def test_retracting_unknown_log_raises(manager):
    with pytest.raises(LogNotFound):
        manager.retract_log(999)


def test_retracted_log_not_returned_by_new_index(manager):
    manager.add_log("svc", "保留 keepmark")
    gone = manager.add_log("svc", "撤回 dropmark")
    manager.retract_log(gone["id"])
    old = active_id(manager)
    manager.start_rebuild()
    wait_for(lambda: active_id(manager) != old)

    assert manager.search("dropmark", None)["total"] == 0
    assert manager.search("keepmark", None)["total"] == 1
    assert manager.search(None, "svc")["total"] == 1
    # doc_count counts the physical FTS rows of the generation (the retracted
    # row is kept there so activation bookkeeping does not race); the live
    # count exposed to users is total_logs.
    assert manager.status()["active"]["doc_count"] == 2
    assert manager.status()["total_logs"] == 1


def test_row_retracted_after_builder_read_it_still_completes_and_stays_hidden(manager):
    """Builder read the row before the retraction; the rebuild still activates."""
    for i in range(5):
        manager.add_log("svc", f"批次日志 batch{i}")
    marked = manager.add_log("svc", "将被撤回 latemark")
    old = active_id(manager)
    calls = {"n": 0}

    def hook(event, payload):
        if event == "batch":
            calls["n"] += 1
            # Fetching already happened for this batch; retract now so the
            # tokens are inserted into the new table after the retraction.
            manager.retract_log(marked["id"])

    manager.build_hook = hook
    manager.start_rebuild()
    wait_for(lambda: active_id(manager) != old)
    assert calls["n"] >= 1
    new_active = manager.status()["active"]
    # Retraction does not block activation: the build still switched.
    assert new_active["status"] == "active"
    assert manager.search("latemark", None)["total"] == 0
    assert manager.search("batch", None)["total"] == 5
    # The stale FTS row may be present, but the generation is complete for
    # every live log and no retracted log is visible.
    assert manager.search(None, "svc")["total"] == 5


def test_row_retracted_during_batch_read_still_completes_rebuild(manager):
    """Retraction between ``_fetch_batch`` and ``_index_batch`` must not abort activation."""
    for i in range(3):
        manager.add_log("svc", f"竞态 race{i}")
    victim = manager.add_log("svc", "竞态 victimmark")
    old = active_id(manager)
    original_index_batch = manager._index_batch

    def index_batch_then_retract(generation_id, rows):
        original_index_batch(generation_id, rows)
        # Simulates a request thread retracting a row this build just wrote;
        # with an unqualified COUNT check this is where activation used to fail.
        manager.retract_log(victim["id"])

    manager._index_batch = index_batch_then_retract
    try:
        manager.start_rebuild()
        wait_for(lambda: active_id(manager) != old)
    finally:
        manager._index_batch = original_index_batch
    assert manager.search("victimmark", None)["total"] == 0
    assert manager.search("race", None)["total"] == 3


def test_failed_retraction_write_keeps_log_and_index(manager):
    log = manager.add_log("svc", "写入失败 failmark")
    manager.add_log("svc", "其他 othermark")
    old = active_id(manager)
    original_write_tx = manager._write_tx

    def failing_write_tx():
        raise sqlite3.OperationalError("磁盘已满")

    manager._write_tx = failing_write_tx
    try:
        with pytest.raises(sqlite3.OperationalError):
            manager.retract_log(log["id"])
    finally:
        manager._write_tx = original_write_tx

    # Original log and active index are untouched and still searchable.
    assert manager.search("failmark", None)["total"] == 1
    assert manager.search("othermark", None)["total"] == 1
    assert manager.search(None, "svc")["total"] == 2
    assert manager.services() == [{"service": "svc", "count": 2}]
    assert manager.status()["total_logs"] == 2
    assert active_id(manager) == old
    # A later retraction succeeds normally.
    assert manager.retract_log(log["id"])["already_retracted"] is False
    assert manager.search("failmark", None)["total"] == 0


def test_retraction_rolls_back_if_the_update_fails(manager):
    """A failing statement inside the transaction must not leave deleted_at set."""
    log = manager.add_log("svc", "事务回滚 txmark")
    with manager._write_tx() as conn:
        conn.execute(
            "CREATE TRIGGER block_retract BEFORE UPDATE OF deleted_at ON logs "
            "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
        )
    try:
        with pytest.raises(sqlite3.IntegrityError):
            manager.retract_log(log["id"])
    finally:
        with manager._write_tx() as conn:
            conn.execute("DROP TRIGGER block_retract")
    assert manager.search("txmark", None)["total"] == 1
    assert manager.retract_log(log["id"])["already_retracted"] is False


def test_doc_count_tracks_fts_rows_not_live_logs(manager):
    log = manager.add_log("svc", "计数 countmark")
    assert manager.status()["active"]["doc_count"] == 1
    manager.retract_log(log["id"])
    # doc_count counts physical index rows (needed for activation
    # bookkeeping); total_logs is the user-facing live count.
    assert manager.status()["active"]["doc_count"] == 1
    assert manager.status()["total_logs"] == 0
    manager.add_log("svc", "再计数 countmark2")
    assert manager.status()["active"]["doc_count"] == 2
    assert manager.status()["total_logs"] == 1


def test_activation_rejects_index_missing_a_live_log(manager):
    """The completeness check must still catch a genuinely broken build."""
    manager.add_log("svc", "完整性 completeness")
    old = active_id(manager)
    original_index_batch = manager._index_batch

    def index_batch_but_drop_row(generation_id, rows):
        original_index_batch(generation_id, rows)
        generation = manager._get_generation(generation_id)
        with manager._write_tx() as conn:
            conn.execute(
                f'DELETE FROM "{generation["fts_table"]}" WHERE rowid = ?',
                (rows[-1]["id"],),
            )

    manager._index_batch = index_batch_but_drop_row
    try:
        manager.start_rebuild()
        wait_for(lambda: manager.status()["latest"]["status"] == "failed")
    finally:
        manager._index_batch = original_index_batch
    assert active_id(manager) == old
    assert "不完整" in manager.status()["latest"]["error"]
    assert manager.search("completeness", None)["total"] == 1


def test_inflight_search_sees_committed_state_once(manager):
    manager.add_log("svc", "撤回前 snapshotmark")
    during = threading.Event()
    resume = threading.Event()
    seen = {}

    def search_hook(active):
        if not during.is_set():
            during.set()
            # Pause after the search pinned its read snapshot; the retraction
            # commits while it is paused.
            resume.wait(5)

    manager.search_hook = search_hook
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(manager.search, "snapshotmark", None)
        assert during.wait(5)
        assert manager.retract_log(1)["already_retracted"] is False
        resume.set()
        seen = pending.result(timeout=10)

    # The search that started before the commit keeps the pre-commit view...
    assert seen["total"] == 1
    # ...and every search started afterwards sees the retraction.
    assert manager.search("snapshotmark", None)["total"] == 0


def test_retraction_while_build_frozen_lets_rebuild_activate(manager):
    """Retraction must not be blocked by, nor block, a concurrent rebuild."""
    for i in range(20):
        manager.add_log("svc", f"并发重建 concurrent{i}")
    victim = manager.add_log("svc", "并发重建 frozenmark")
    old = active_id(manager)
    reached = threading.Event()
    release = threading.Event()

    def hook(event, payload):
        if event == "batch":
            reached.set()
            release.wait(10)

    manager.build_hook = hook
    manager.start_rebuild()
    assert reached.wait(5)
    assert manager.retract_log(victim["id"])["already_retracted"] is False
    # Ingestion and search still work while the builder is paused.
    manager.add_log("svc", "并发写入 duringbuild")
    assert manager.search("duringbuild", None)["total"] == 1
    release.set()
    wait_for(lambda: active_id(manager) != old)
    assert manager.search("frozenmark", None)["total"] == 0
    assert manager.search(None, "svc")["total"] == 21


def test_existing_database_is_migrated_for_retraction(tmp_path):
    """A DB created before this feature has no deleted_at and no FTS rows for
    a new column; opening it must add the column without touching the index."""
    db = str(tmp_path / "old.db")
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE index_generations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                activated_at TEXT,
                finished_at TEXT,
                start_id INTEGER NOT NULL DEFAULT 0,
                build_cursor INTEGER NOT NULL DEFAULT 0,
                indexed_through INTEGER NOT NULL DEFAULT 0,
                doc_count INTEGER NOT NULL DEFAULT 0,
                fts_table TEXT NOT NULL,
                error TEXT
            );
            CREATE VIRTUAL TABLE fts_0123456789abcdef USING fts5(tokens);
            INSERT INTO logs(service, body, created_at)
                VALUES ('old', '历史日志 legacy', '2026-01-01T00:00:00+00:00');
            INSERT INTO fts_0123456789abcdef(rowid, tokens) VALUES (1, '历史 史日 日志 legacy');
            INSERT INTO index_generations
                (status, created_at, start_id, build_cursor, indexed_through,
                 doc_count, fts_table)
                VALUES ('active', '2026-01-01T00:00:00+00:00', 1, 1, 1, 1,
                        'fts_0123456789abcdef');
            """
        )
    finally:
        conn.close()

    idx = IndexManager(db)
    idx.initialize()
    try:
        assert idx.search("legacy", None)["total"] == 1
        assert idx.retract_log(1)["already_retracted"] is False
        assert idx.search("legacy", None)["total"] == 0
        assert idx.services() == []
    finally:
        idx.shutdown()

    reopened = IndexManager(db)
    reopened.initialize()
    try:
        assert reopened.search("legacy", None)["total"] == 0
        assert reopened.retract_log(1)["already_retracted"] is True
    finally:
        reopened.shutdown()


def test_retraction_after_restart_stays_hidden(manager):
    log = manager.add_log("svc", "重启后 restartmark")
    manager.retract_log(log["id"])
    db = manager.db_path
    manager.shutdown()

    fresh = IndexManager(db)
    fresh.initialize()
    try:
        assert fresh.search("restartmark", None)["total"] == 0
        assert fresh.search(None, "svc")["total"] == 0
        assert fresh.status()["total_logs"] == 0
    finally:
        fresh.shutdown()


def test_retracted_log_is_not_counted_in_pagination(manager):
    ids = [manager.add_log("svc", f"分页 page{i}")["id"] for i in range(6)]
    manager.retract_log(ids[2])
    first = manager.search("page", None, limit=2, offset=0)
    second = manager.search("page", None, limit=2, offset=2)
    third = manager.search("page", None, limit=2, offset=4)
    got = [item["id"] for item in first["items"] + second["items"] + third["items"]]
    assert got == sorted([i for i in ids if i != ids[2]], reverse=True)
    assert first["total"] == 5 and third["total"] == 5
