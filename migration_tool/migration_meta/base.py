from abc import ABC, abstractmethod
from typing import Optional

from sqlalchemy import Connection

from migration_tool.logger.mix_in import LoggerMixIn


class MigrationMeta(LoggerMixIn, ABC):
    @abstractmethod
    def _try_get_target_connection(self) -> Optional[Connection]:
        raise NotImplementedError()

    @abstractmethod
    def _check_meta_storage(self):
        raise NotImplementedError()

    @abstractmethod
    def _get_current_version(self) -> int:
        raise NotImplementedError()

    def check_migration_version(self) -> Optional[int]:
        connection = self._try_get_target_connection()

        if connection is None:
            return None
        connection.close()

        self._check_meta_storage()

        return self._get_current_version()

    @abstractmethod
    def update_migration_version(self, new_version: int, target_conn: Optional[Connection] = None):
        raise NotImplementedError()
