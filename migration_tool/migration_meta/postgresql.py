from pathlib import Path
from typing import Optional

from retry import retry
from sqlalchemy import Connection, Engine, Inspector, text

from migration_tool.migration_meta.base import MigrationMeta

ROOT_PATH = Path(__file__).parent.parent.parent


class PostgreSQLMigrationMeta(MigrationMeta):
    MIGRATION_META_SCHEMA = 'version_meta'
    META_SCRIPT = ROOT_PATH / 'raw' / 'postgresql' / 'meta.sql'
    SELECT_VERSION_SCRIPT = 'SELECT version FROM version_meta.current_version'
    UPDATE_VERSION_SCRIPT = ';CALL version_meta.sp_update_db_version({version});'

    def __init__(self, target_engine: Engine, target_conn: Optional[Connection] = None):
        self._target_conn = target_conn
        self._target_engine = target_engine

    def _try_get_target_connection(self) -> Optional[Connection]:
        if self._target_conn is None or self._target_conn.closed:
            try:
                self._target_conn = self._target_engine.connect()
            except Exception as e:
                self._target_conn = None

        return self._target_conn

    @retry(tries=3, delay=10, backoff=2)
    def _check_meta_storage(self) -> bool:
        inspector = Inspector.from_engine(self._target_engine)
        schemas = inspector.get_schema_names()

        if self.MIGRATION_META_SCHEMA in schemas:
            return True

        self.logger.info(f"Meta storage in schema not found: {self.MIGRATION_META_SCHEMA}")
        target_conn = self._try_get_target_connection()
        if target_conn is None:
            # raise ConnectionError("Can't establish connection for target DB.")
            return False

        with open(self.META_SCRIPT, 'r', encoding="utf-8") as file:
            script = file.read()

        self.logger.info(f"Run meta initialization script")
        with target_conn as conn:
            try:
                conn.execute(text(script))
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Receiver error on meta initialization: {e}")
                raise

        self.logger.info(f"Meta initialization complete")
        return True

    def _get_current_version(self) -> int:
        conn = self._try_get_target_connection()
        if conn is None:
            raise ConnectionError("Can't establish connection for target DB.")

        curr_version = conn.execute(text(self.SELECT_VERSION_SCRIPT)).fetchall()
        curr_version = list(curr_version)[0][0]

        return curr_version

    def update_migration_version(self, new_version: int, target_conn: Optional[Connection] = None):
        if not self._check_meta_storage():
            self.logger.warning(f"Skipping tracking of version: {new_version} due of problems with meta_storage")
            return

        conn = target_conn
        if conn is None:
            conn = self._try_get_target_connection()
        if conn is None:
            raise ConnectionError("Can't establish connection for target DB.")

        # TODO refactor with sql bind params
        sql_version = text(str(self.UPDATE_VERSION_SCRIPT).format(version=new_version))
        conn.execute(sql_version)
        self.logger.info(f"Meta version updated to: {new_version}")
