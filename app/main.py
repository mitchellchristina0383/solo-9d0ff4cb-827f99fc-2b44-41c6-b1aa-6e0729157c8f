"""FastAPI application for the log search service."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from .indexer import IndexManager, IndexUnavailable, RebuildInProgress

STATIC_DIR = Path(__file__).resolve().parent / "static"


class LogIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    service: str = Field(min_length=1, max_length=100)
    body: str = Field(min_length=1, max_length=20_000)


def create_app(db_path: str | None = None, *, manager: IndexManager | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("LOGDB_PATH", "/data/logs.db")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        idx = manager or IndexManager(db_path)
        await run_in_threadpool(idx.initialize)
        app.state.index_manager = idx
        try:
            yield
        finally:
            idx.shutdown()

    app = FastAPI(title="日志检索", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    def get_manager() -> IndexManager:
        return app.state.index_manager

    @app.get("/", include_in_schema=False)
    def index_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/healthz")
    def healthz():
        idx = get_manager()
        if not idx.health():
            raise HTTPException(status_code=503, detail="数据库或索引不可用")
        return {"ok": True}

    @app.post("/api/logs", status_code=201)
    async def add_log(payload: LogIn):
        idx = get_manager()
        return await run_in_threadpool(idx.add_log, payload.service, payload.body)

    @app.get("/api/logs")
    async def search_logs(
        q: Annotated[str | None, Query(max_length=500)] = None,
        service: Annotated[str | None, Query(max_length=100)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        idx = get_manager()
        try:
            result = await run_in_threadpool(idx.search, q, service, limit=limit, offset=offset)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IndexUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return result

    @app.get("/api/services")
    async def list_services():
        idx = get_manager()
        return {"items": await run_in_threadpool(idx.services)}

    @app.post("/api/reindex", status_code=202)
    async def start_reindex():
        idx = get_manager()
        try:
            generation = await run_in_threadpool(idx.start_rebuild)
        except RebuildInProgress as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except IndexUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"building": generation}

    @app.get("/api/status")
    async def status():
        idx = get_manager()
        return await run_in_threadpool(idx.status)

    return app


app = create_app()
