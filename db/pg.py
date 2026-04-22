"""Postgres connection helper for Neon (via Vercel Storage).

In Vercel, DATABASE_URL / POSTGRES_URL are injected automatically.
Locally we load them from .env via python-dotenv.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass


def _url() -> str:
    for key in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_URL_NON_POOLING"):
        v = os.environ.get(key)
        if v:
            return v
    raise RuntimeError("No DATABASE_URL / POSTGRES_URL configured")


def connect(autocommit: bool = True, row_factory=dict_row) -> psycopg.Connection:
    return psycopg.connect(_url(), autocommit=autocommit, row_factory=row_factory)


@contextmanager
def cursor():
    conn = connect()
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def init_schema():
    schema = (Path(__file__).resolve().parent / "schema_pg.sql").read_text()
    with cursor() as cur:
        cur.execute(schema)
