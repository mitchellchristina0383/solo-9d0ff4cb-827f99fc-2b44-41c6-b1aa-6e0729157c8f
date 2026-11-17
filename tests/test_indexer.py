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
