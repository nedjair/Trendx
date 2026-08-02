from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from trendx.config import settings


class EngineWrapper:
    def __init__(self, name: str, dsn: str, pool_size: int = 5, max_overflow: int = 10) -> None:
        self.name = name
        self.engine = create_engine(
            dsn,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            connect_args={"connect_timeout": 10},
        )
        logger.debug("Engine '{}' created (pool={}, overflow={})", name, pool_size, max_overflow)

    def dispose(self) -> None:
        self.engine.dispose()
        logger.debug("Engine '{}' disposed", self.name)


class DatabaseManager:
    def __init__(self) -> None:
        self._engines: dict[str, EngineWrapper] = {}
        self._sessionmakers: dict[str, sessionmaker[Session]] = {}

    def register(self, name: str, dsn: str, pool_size: int = 5, max_overflow: int = 10) -> None:
        if name in self._engines:
            logger.warning("Engine '{}' already registered, skipping", name)
            return
        wrapper = EngineWrapper(name, dsn, pool_size=pool_size, max_overflow=max_overflow)
        self._engines[name] = wrapper
        self._sessionmakers[name] = sessionmaker(bind=wrapper.engine, expire_on_commit=False)
        logger.info("Registered engine '{}'", name)

    def get_engine(self, name: str):
        wrapper = self._engines.get(name)
        if wrapper is None:
            msg = f"Engine '{name}' is not registered"
            raise ValueError(msg)
        return wrapper.engine

    def get_sessionmaker(self, name: str) -> sessionmaker[Session]:
        maker = self._sessionmakers.get(name)
        if maker is None:
            msg = f"Sessionmaker for '{name}' is not registered"
            raise ValueError(msg)
        return maker

    def get_catalog_engine(self):
        return self.get_engine("catalog")

    def get_analytics_engine(self):
        return self.get_engine("analytics")

    @contextmanager
    def get_session(self, db_name: str = "catalog") -> Generator[Session, None, None]:
        maker = self.get_sessionmaker(db_name)
        session: Session = maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def check_connection(self, db_name: str = "catalog") -> bool:
        engine = self.get_engine(db_name)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.debug("Connection check passed for '{}'", db_name)
            return True
        except Exception as exc:
            logger.error("Connection check failed for '{}': {}", db_name, exc)
            return False

    def check_all_connections(self) -> dict[str, bool]:
        results: dict[str, bool] = {}
        for name in self._engines:
            results[name] = self.check_connection(name)
        return results

    def dispose_all(self) -> None:
        for name, wrapper in self._engines.items():
            wrapper.dispose()
        self._engines.clear()
        self._sessionmakers.clear()
        logger.info("All engines disposed")


manager = DatabaseManager()


def _register_default_engines() -> None:
    manager.register("catalog", settings.catalog_dsn_app())
    manager.register("analytics", settings.analytics_dsn_app())

    logger.info("Default engines registered (catalog, analytics)")


_register_default_engines()


def get_catalog_engine():
    return manager.get_catalog_engine()


def get_analytics_engine():
    return manager.get_analytics_engine()


def get_session(db_name: str = "catalog") -> Generator[Session, None, None]:
    return manager.get_session(db_name)
