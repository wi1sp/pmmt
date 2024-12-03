from typing import Optional

from retry import retry
from sqlalchemy import create_engine, text, Connection

from migration_tool.db_migration.base import DBMigrationRunner, ExecMigration
from migration_tool.db_types import DBType
from migration_tool.logger.mix_in import LoggerMixIn
from migration_tool.migration_config import MigrationConfig
from migration_tool.migration_files.loader.base import MigrationFilesLoader
from migration_tool.migration_meta.postgresql import PostgreSQLMigrationMeta

APP_NAME = 'migration-tool'


class PostgreSQLMigrationRunner(DBMigrationRunner):
    DEFAULT_DB_NAME = 'postgres'
    RETRY_LOGGER = LoggerMixIn.init_logger(f"retry")

    def __init__(self, config: MigrationConfig, files_loader: MigrationFilesLoader):
        if config.db_type != DBType.Postgresql:
            raise ValueError("Given not match config for postgresql migration env")

        self._config = config
        self._files_loader = files_loader

        self.target_engine = create_engine(
            self.target_uri,
            connect_args={"application_name": APP_NAME},
            # echo=True,
            pool_recycle=30,
            pool_pre_ping=True,
        )
        self.default_engine = create_engine(
            self.default_uri,
            connect_args={"application_name": APP_NAME},
            isolation_level="AUTOCOMMIT",
            # echo=True,
            pool_recycle=30,
            pool_pre_ping=True,
        )

        self._migration_meta = PostgreSQLMigrationMeta(
            target_engine=self.target_engine,
        )
        self._target_conn: Optional[Connection] = None

    @property
    def target_conn(self):
        if self._target_conn is None or self._target_conn.closed:
            self._target_conn = self.target_engine.connect()

        self.shared_target_conn = self._target_conn
        return self._target_conn

    @property
    def target_uri(self):
        return (
            f"postgresql+psycopg2://"
            f"{self._config.db_settings.DB_USER}:{self._config.db_settings.DB_USER_PASSWORD}@"
            f"{self._config.db_settings.DB_HOST}:{self._config.db_settings.DB_PORT}/{self._config.db_name}"
        )

    @property
    def default_uri(self):
        return (
            f"postgresql+psycopg2://"
            f"{self._config.db_settings.DB_USER}:{self._config.db_settings.DB_USER_PASSWORD}@"
            f"{self._config.db_settings.DB_HOST}:{self._config.db_settings.DB_PORT}/{self.DEFAULT_DB_NAME}"
        )

    @property
    def migration_files_loader(self) -> MigrationFilesLoader:
        return self._files_loader

    @property
    def migration_meta(self) -> PostgreSQLMigrationMeta:
        return self._migration_meta

    @retry(tries=3, delay=10, backoff=2, logger=RETRY_LOGGER)
    def _execute_db_manage_query(self, query: str):
        default_conn = self.default_engine.connect()
        sql = text(query)

        default_conn.execute(sql)
        default_conn.commit()

        default_conn.close()

    @retry(tries=3, delay=10, backoff=2, logger=RETRY_LOGGER)
    def _execute_migration_query(self, migration: ExecMigration, query: str):
        with self.target_conn as conn:
            try:
                sql = text(query)
                conn.execute(sql)
                self._update_version_for_migration(migration)
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Received error on migration execute: {e}")
                raise

            conn.commit()
