from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from trendx.config import settings


# Budget connexions trendx_app (AGENTS §2, rôle CONNECTION LIMIT 24) :
#   2 moteurs (catalog + analytics) x (pool 2 + overflow 1) = 6 connexions max
#   par processus ; API (2 workers) = 12, worker = 6, total = 18 <= 24.
# Moteur tb_readonly (rôle trendx_ro, LIMIT 5) : pool 1 + overflow 0 par
#   processus ; API (2 workers) = 2, worker = 1, total = 3 <= 5.
DEFAULT_POOL_SIZE = 2
DEFAULT_MAX_OVERFLOW = 1


class EngineWrapper:
    def __init__(
        self,
        name: str,
        dsn: str,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        search_path: str | None = None,
    ) -> None:
        self.name = name
        connect_args: dict = {"connect_timeout": 10}
        if search_path:
            connect_args["options"] = "-c search_path=" + search_path
        self.engine = create_engine(
            dsn,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            connect_args=connect_args,
        )
        logger.debug(
            "Engine '{}' created (pool={}, overflow={}, search_path={})",
            name,
            pool_size,
            max_overflow,
            search_path,
        )

    def dispose(self) -> None:
        self.engine.dispose()
        logger.debug("Engine '{}' disposed", self.name)


class DatabaseManager:
    def __init__(self) -> None:
        self._engines: dict[str, EngineWrapper] = {}
        self._sessionmakers: dict[str, sessionmaker[Session]] = {}

    def register(
        self,
        name: str,
        dsn: str,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        search_path: str | None = None,
    ) -> None:
        if name in self._engines:
            logger.warning("Engine '{}' already registered, skipping", name)
            return
        wrapper = EngineWrapper(
            name, dsn, pool_size=pool_size, max_overflow=max_overflow, search_path=search_path
        )
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

    def get_tb_readonly_engine(self):
        return self.get_engine("tb_readonly")

    def engine_search_paths(self) -> dict[str, str]:
        paths: dict[str, str] = {}
        for name, wrapper in self._engines.items():
            try:
                with wrapper.engine.connect() as conn:
                    row = conn.execute(text("SHOW search_path")).fetchone()
                    paths[name] = str(row[0]) if row and row[0] else ""
            except Exception as exc:
                paths[name] = f"ERROR: {exc}"
        return paths

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
    manager.register("catalog", settings.catalog_dsn_app(), search_path="trendx_catalog,public")
    manager.register(
        "analytics", settings.analytics_dsn_app(), search_path="trendx_analytics,public"
    )
    ro_dsn = settings.tb_db_readonly_dsn()
    if ro_dsn:
        manager.register(
            "tb_readonly", ro_dsn, pool_size=1, max_overflow=0, search_path="public"
        )
        logger.info("Registered read-only ThingsBoard engine (tb_readonly, pool=1/0)")

    logger.info("Default engines registered (catalog, analytics[, tb_readonly])")


_register_default_engines()


def get_catalog_engine():
    return manager.get_catalog_engine()


def get_analytics_engine():
    return manager.get_analytics_engine()


def get_tb_readonly_engine():
    return manager.get_tb_readonly_engine()


def get_session(db_name: str = "catalog") -> Generator[Session, None, None]:
    return manager.get_session(db_name)
