"""审计接收端（本地演示用）。

接收“部署完成通知”，按 (env, release_no) 幂等入库，并提供回执查询。
- 相同 (env, release_no) 且内容一致的重试：返回既有回执，不新增记录；
- 相同 (env, release_no) 但内容不同：409 冲突；
- 按环境暂停/恢复：暂停期间新发布号明确拒收（423），已入账回执仍可查询；
  拒收与新入账互斥：一个请求要么已提交入库并拿到可查回执，要么明确拒收且不落库；
- 暂停开关持久化在 SQLite，重启后保留；写入失败回滚，原状态不变；
- 故障注入（仅演示）：simulate=lose_response（入库后丢响应）/ slow（入库后慢响应）；
  暂停开关支持 simulate=write_failure（事务内失败并回滚，用于验证不改变原状态）。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/receiver.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
VALID_SIMULATE = {"lose_response", "slow"}
VALID_PAUSE_SIMULATE = {"write_failure"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 暂停开关与环境同库持久化：重启后仍保留。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS env_pauses (
                env TEXT PRIMARY KEY,
                paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
                updated_at TEXT NOT NULL
            )
            """
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, rid: int) -> dict:
    row = conn.execute("SELECT * FROM receipts WHERE id=?", (rid,)).fetchone()
    return row_to_dict(row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="审计接收端", lifespan=lifespan)


class ReceiptIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None  # lose_response | slow


class PauseIn(BaseModel):
    env: str
    paused: bool
    simulate: str | None = None  # write_failure（仅演示）


def pause_state(conn: sqlite3.Connection, env: str) -> dict:
    row = conn.execute("SELECT * FROM env_pauses WHERE env=?", (env,)).fetchone()
    if row is None:
        return {"env": env, "paused": False, "updated_at": None}
    return {"env": row["env"], "paused": bool(row["paused"]), "updated_at": row["updated_at"]}


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/receipts")
def create_receipt(body: ReceiptIn):
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})
    if body.simulate is not None and body.simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        # 暂停检查、幂等查重与插入在同一个写事务内完成：
        # - 先入账者的回执已提交且可查，暂停请求随后看到的就是「不影响已入账」；
        # - 先暂停成功后，后到的写入在事务内看到开关即明确拒收，绝不落库；
        # - 已入账的同键同内容重试仍返回原回执、异内容仍 409（不受暂停影响）。
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    # 同内容重试：幂等返回既有回执，绝不重复记账
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })

            state = pause_state(conn, env)
            if state["paused"]:
                conn.execute("ROLLBACK")
                return JSONResponse(status_code=423, content={
                    "detail": f"环境 {env} 已暂停入账，新发布号拒收；已入账回执仍可查询",
                    "code": "ENV_PAUSED",
                    "env": env,
                    "paused": True,
                    "paused_at": state["updated_at"],
                })

            cur = conn.execute(
                "INSERT INTO receipts (env, release_no, result, content_hash, received_at) VALUES (?,?,?,?,?)",
                (env, release_no, result, ch, now_iso()),
            )
            rid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        receipt = fetch_dict(conn, rid)
        # —— 故障注入（仅演示）：记录已提交入库，但调用方拿不到回执 ——
        if body.simulate == "lose_response":
            return JSONResponse(status_code=500, content={"detail": "模拟故障：记录已入库，响应丢失"})
        if body.simulate == "slow":
            time.sleep(6)  # 长于发送端 3s 超时，回执实际已生成
        return JSONResponse(status_code=201, content=receipt)
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"接收端写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/receipts")
def list_or_get_receipts(env: str | None = None, release_no: str | None = None):
    conn = connect()
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is None:
                state = pause_state(conn, env)
                return JSONResponse(status_code=404, content={
                    "detail": "无此记录",
                    "paused": state["paused"],
                    "paused_at": state["updated_at"],
                })
            return row_to_dict(row)
        rows = conn.execute("SELECT * FROM receipts ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/pauses")
def list_pauses():
    """列出所有已知环境（台账中出现的 + 已设置开关的）及其暂停状态。"""
    conn = connect()
    try:
        envs = [r["env"] for r in conn.execute(
            "SELECT env FROM receipts UNION SELECT env FROM env_pauses ORDER BY env"
        ).fetchall()]
        return {"items": [pause_state(conn, e) for e in envs]}
    finally:
        conn.close()


@app.get("/api/pauses/{env}")
def get_pause(env: str):
    conn = connect()
    try:
        return pause_state(conn, env)
    finally:
        conn.close()


@app.post("/api/pauses")
def set_pause(body: PauseIn):
    """按环境暂停/恢复入账。写入失败则回滚并保持原状态。"""
    env = body.env.strip()
    if not env:
        return JSONResponse(status_code=400, content={"detail": "env 不能为空"})
    if len(env) > 64:
        return JSONResponse(status_code=400, content={"detail": "env 长度不能超过 64 字符"})
    if body.simulate is not None and body.simulate not in VALID_PAUSE_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_PAUSE_SIMULATE)}"})

    conn = connect()
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error:
            return JSONResponse(status_code=500, content={
                "detail": "暂停写入失败（无法开启事务），原状态未改变",
                "unchanged": pause_state(conn, env),
            })
        try:
            before = pause_state(conn, env)
            if body.simulate == "write_failure":
                raise sqlite3.OperationalError("模拟写入失败（事务回滚）")
            conn.execute(
                """
                INSERT INTO env_pauses (env, paused, updated_at) VALUES (?,?,?)
                ON CONFLICT(env) DO UPDATE SET paused=excluded.paused, updated_at=excluded.updated_at
                """,
                (env, 1 if body.paused else 0, now_iso()),
            )
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            return JSONResponse(status_code=500, content={
                "detail": "暂停写入失败，原状态未改变",
                "unchanged": before,
            })
        after = pause_state(conn, env)
        return {"ok": True, "before": before, "state": after}
    finally:
        conn.close()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
