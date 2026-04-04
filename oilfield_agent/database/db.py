from __future__ import annotations
from contextlib import contextmanager
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.settings import settings

engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def run_query(sql: str, params: Optional[dict] = None) -> list:
    with get_db() as db:
        result = db.execute(text(sql), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def run_scalar(sql: str, params: Optional[dict] = None):
    with get_db() as db:
        result = db.execute(text(sql), params or {})
        row = result.fetchone()
        return row[0] if row else None


def execute_dml(sql: str, params: Optional[dict] = None) -> int:
    with get_db() as db:
        result = db.execute(text(sql), params or {})
        return result.rowcount
