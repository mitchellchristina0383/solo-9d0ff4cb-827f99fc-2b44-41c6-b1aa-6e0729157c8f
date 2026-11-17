"""HTTP-level tests, using a real server thread so writes can race a rebuild."""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from app.indexer import IndexManager
from app.main import create_app


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, tmp_path):
        self.manager = IndexManager(str(tmp_path / "http.db"))
        self.port = free_port()
        self.app = create_app(manager=self.manager)
        self.server = uvicorn.Server(
            uvicorn.Config(
                self.app,
                host="127.0.0.1",
                port=self.port,
                log_level="warning",
                lifespan="on",
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.01)
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=10)
        return self

    def __exit__(self, *exc):
        self.client.close()
        self.server.should_exit = True
        self.thread.join(timeout=10)

    def wait_active_changed(self, old, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            data = self.client.get("/api/status").json()
            if data["active"] and data["active"]["id"] != old:
                return data
            time.sleep(0.02)
        raise AssertionError("rebuild did not activate in time")


@pytest.fixture()
def server(tmp_path):
    with Server(tmp_path) as running:
        yield running


def test_pages_and_health(server):
    assert server.client.get("/").status_code == 200
    assert "日志检索" in server.client.get("/").text
    assert server.client.get("/static/app.js").status_code == 200
    assert server.client.get("/healthz").json() == {"ok": True}


def test_ingest_search_and_status_over_http(server):
    response = server.client.post(
        "/api/logs", json={"service": "auth-api", "body": "用户登录失败 error 500"}
    )
    assert response.status_code == 201
    log_id = response.json()["id"]

    found = server.client.get("/api/logs", params={"q": "登录失败"}).json()
    assert found["total"] == 1
    assert found["items"][0]["id"] == log_id
    assert found["mode"] == "fts"

    by_service = server.client.get(
        "/api/logs", params={"service": "auth-api", "q": "500"}
    ).json()
    assert by_service["total"] == 1

    wrong_service = server.client.get(
        "/api/logs", params={"service": "other", "q": "500"}
    ).json()
    assert wrong_service["total"] == 0

    assert server.client.get("/api/logs").status_code == 400
    assert server.client.post("/api/logs", json={"service": "", "body": "x"}).status_code == 422
    status = server.client.get("/api/status").json()
    assert status["active"]["doc_count"] == 1
    assert status["total_logs"] == 1


def test_reindex_over_http_with_interleaved_writes(server):
    for i in range(60):
        server.client.post("/api/logs", json={"service": "pay", "body": f"支付超时 batch{i}"})
    old = server.client.get("/api/status").json()["active"]["id"]
    held = threading.Event()
    seen_batches = []

    # Hold the builder inside its first batch while HTTP writes land.  This
    # makes the interleaving deterministic instead of timing-dependent.
    def hook(event, payload):
        if event == "batch":
            seen_batches.append(payload)
            held.wait(10)

    server.manager.build_hook = hook
    assert server.client.post("/api/reindex").status_code == 202
    assert server.client.post("/api/reindex").status_code == 409

    written = []
    for i in range(40):
        response = server.client.post(
            "/api/logs", json={"service": "pay", "body": f"重建中写入 live{i}"}
        )
        assert response.status_code == 201
        written.append(response.json()["id"])
        # Searches stay available and use a single generation.
        result = server.client.get("/api/logs", params={"q": "支付"}).json()
        assert result["generation"]["id"] == old  # not switched mid-build
        assert result["total"] >= 60

    assert seen_batches, "builder never ran while writes were in flight"
    held.set()
    data = server.wait_active_changed(old)
    assert data["building"] is None
    assert data["active"]["doc_count"] == 100

    result = server.client.get("/api/logs", params={"q": "重建中写入", "limit": 200}).json()
    assert result["total"] == 40
    assert {item["id"] for item in result["items"]} == set(written)
    assert result["generation"]["id"] == data["active"]["id"]

    everything = server.client.get("/api/logs", params={"service": "pay", "limit": 200}).json()
    assert everything["total"] == 100
    ids = [item["id"] for item in everything["items"]]
    assert len(ids) == len(set(ids))


def test_failed_rebuild_reports_error_and_keeps_serving(server):
    server.client.post("/api/logs", json={"service": "db", "body": "连接池耗尽 poolmark"})
    old = server.client.get("/api/status").json()["active"]["id"]

    def hook(event, payload):
        if event == "batch":
            raise RuntimeError("磁盘写入失败")

    server.manager.build_hook = hook
    server.client.post("/api/reindex")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = server.client.get("/api/status").json()
        if status["latest"]["status"] == "failed":
            break
        time.sleep(0.02)
    else:
        raise AssertionError("failure was not reported")

    assert status["active"]["id"] == old
    assert status["building"] is None
    assert "磁盘写入失败" in status["latest"]["error"]
    assert server.client.get("/api/logs", params={"q": "poolmark"}).json()["total"] == 1
    # Retry is allowed once the failed build has been cleaned up.
    server.manager.build_hook = None
    assert server.client.post("/api/reindex").status_code == 202
    server.wait_active_changed(old)


def test_restart_via_lifespan_keeps_old_index(tmp_path):
    """New process instance over the same DB keeps the last complete index."""
    manager = IndexManager(str(tmp_path / "restart.db"))
    manager.initialize()
    manager.add_log("svc", "重启前 before-restart")
    old = manager.status()["active"]["id"]
    # Leave a half-built generation behind, as a crash would.
    manager.chunk_size = 1
    gate = threading.Event()
    manager.build_hook = lambda event, payload: gate.wait(5) if event == "batch" else None
    manager.start_rebuild()
    deadline = time.monotonic() + 5
    while manager.status()["building"] is None and time.monotonic() < deadline:
        time.sleep(0.01)

    fresh = IndexManager(str(tmp_path / "restart.db"))
    fresh.initialize()
    try:
        status = fresh.status()
        assert status["active"]["id"] == old
        assert status["building"] is None
        assert status["latest"]["status"] == "failed"
        assert fresh.search("before", None)["total"] == 1
    finally:
        fresh.shutdown()
        gate.set()
        manager.shutdown()
