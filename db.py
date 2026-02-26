#!/usr/bin/env python3
"""
AsyncPG connection pool with synchronous bridge.

Same architecture as the pipeline db.py:
  - One asyncio event loop in a daemon thread
  - asyncpg pool lives on that loop
  - Sync callers use run_coroutine_threadsafe()
"""

import os
import json
import ssl
import time
import logging
import asyncio
import threading
from typing import Any, List, Optional, Sequence, Tuple

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("scraper.db")

_HOST = os.getenv("SUPABASE_HOST", "db.voqfjfngdpcvevbkikud.supabase.co")
_PORT = os.getenv("SUPABASE_PORT", "5432")
_DB   = os.getenv("SUPABASE_DB",   "postgres")
_USER = os.getenv("SUPABASE_USER", "postgres")
_PASS = os.getenv("SUPABASE_PASSWORD", "")
DSN   = f"postgresql://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_DB}"


def _jsonb_binary_encoder(value):
    return b'\x01' + json.dumps(value).encode('utf-8')


def _jsonb_binary_decoder(data):
    return json.loads(data[1:])


async def _init_connection(conn: asyncpg.Connection):
    await conn.execute("SET statement_timeout = '300s'")
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads,
                               schema="pg_catalog", format="text")
    await conn.set_type_codec("json",  encoder=json.dumps, decoder=json.loads,
                               schema="pg_catalog", format="text")
    await conn.set_type_codec("jsonb",
                               encoder=_jsonb_binary_encoder,
                               decoder=_jsonb_binary_decoder,
                               schema="pg_catalog", format="binary")
    await conn.set_type_codec("json",
                               encoder=lambda v: json.dumps(v).encode('utf-8'),
                               decoder=lambda d: json.loads(d),
                               schema="pg_catalog", format="binary")


class ScraperDB:
    _TIMEOUT_SQL = "SET statement_timeout = '300s'"

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=30):
            raise RuntimeError("Database pool failed to initialize within 30s")

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._create_pool())
        self._ready.set()
        self._loop.run_forever()

    async def _create_pool(self):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        self._pool = await asyncpg.create_pool(
            DSN,
            min_size=2,
            max_size=5,
            command_timeout=300,
            init=_init_connection,
            ssl=ssl_ctx,
        )
        logger.info("Database pool ready (min=2, max=5)")

    def _run(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def close(self):
        if self._pool:
            self._run(self._pool.close())
            logger.info("Database pool closed")

    # ── Public sync methods ───────────────────────────────────────────────────

    def execute(self, query: str, *args, statement_timeout: int = None) -> str:
        async def _do():
            async with self._pool.acquire() as conn:
                if statement_timeout is not None:
                    await conn.execute(f"SET statement_timeout = '{statement_timeout}s'")
                else:
                    await conn.execute(self._TIMEOUT_SQL)
                return await conn.execute(query, *args)
        return self._run(_do())

    def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute(self._TIMEOUT_SQL)
                return await conn.fetch(query, *args)
        return self._run(_do())

    def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute(self._TIMEOUT_SQL)
                return await conn.fetchrow(query, *args)
        return self._run(_do())

    def fetchval(self, query: str, *args) -> Any:
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute(self._TIMEOUT_SQL)
                return await conn.fetchval(query, *args)
        return self._run(_do())

    def executemany(self, query: str, args: Sequence[Sequence]) -> None:
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute(self._TIMEOUT_SQL)
                await conn.executemany(query, args)
        return self._run(_do())


# ── Singleton ─────────────────────────────────────────────────────────────────

_db_instance: Optional[ScraperDB] = None
_db_lock = threading.Lock()


def get_db() -> ScraperDB:
    global _db_instance
    if _db_instance is None:
        with _db_lock:
            if _db_instance is None:
                _db_instance = ScraperDB()
                _db_instance.start()
    return _db_instance


def close_db():
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None


def retry_db(fn, max_retries: int = 5, description: str = "db operation"):
    """Execute a DB operation with exponential backoff retry."""
    _logger = logging.getLogger("scraper.retry")
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = min(2 ** attempt, 15)
            _logger.warning(
                f"{description} failed (attempt {attempt + 1}/{max_retries}): "
                f"{type(e).__name__}: {e}. Retrying in {wait}s..."
            )
            time.sleep(wait)
