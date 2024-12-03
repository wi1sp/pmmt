from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property
from typing import List, Optional, Tuple

from retry import retry
from sqlalchemy import Connection

from migration_tool.logger.mix_in import LoggerMixIn
from migration_tool.migration_files.file import MigrationFile
from migration_tool.migration_files.loader.base import MigrationFilesLoader
from migration_tool.migration_meta.base import MigrationMeta


class MigrationType(Enum):
    Up = 'up'
    Down = 'down'


ExecMigration = Tuple[int, MigrationType]


class DBMigrationRunner(LoggerMixIn, ABC):
    RETRY_LOGGER = LoggerMixIn.init_logger(f"retry")
    MIN_MIGRATION_VERSION = 0
    DB_LEVEL_MIGRATIONS = [0]
    NOT_TRACK_IN_META = [
        (0, MigrationType.Down)
    ]
    shared_target_conn: Optional[Connection] = None

    @property
    @abstractmethod
    def migration_files_loader(self) -> MigrationFilesLoader:
        raise NotImplementedError()

    @property
    @abstractmethod
    def migration_meta(self) -> MigrationMeta:
        raise NotImplementedError()

    @cached_property
    def migration_files(self):
        return self.migration_files_loader.load_files_list()

    @cached_property
    def migration_files_map(self):
        files = self.migration_files

        return {
            file.version: file
            for file in files
        }

    @abstractmethod
    def _execute_db_manage_query(self, query: str):
        raise NotImplementedError()

    @abstractmethod
    def _execute_migration_query(self, migration: ExecMigration, query: str):
        raise NotImplementedError()

    @classmethod
    def _find_init_migration(cls, files: List[MigrationFile]):
        find = list(filter(lambda x: x.version == 0, files))
        if len(find) == 0:
            raise ValueError(f"Can't find init migration with version value 0 for create target DB")
        init_migration: MigrationFile = find[0]

        return init_migration

    def _run_init_migration(self, files: List[MigrationFile]):
        init_migration = self._find_init_migration(files)

        self._execute_db_manage_query(init_migration.up_query)

    def build_migration_path(
            self,
            is_drop: bool = False,
            from_version: Optional[int] = None,
            to_version: int = 0
    ) -> List[ExecMigration]:
        if from_version is not None and self.MIN_MIGRATION_VERSION > from_version:
            raise ValueError(
                f"Passed {from_version=} is less "
                f"than min acceptable version value: {self.MIN_MIGRATION_VERSION}"
            )
        if self.MIN_MIGRATION_VERSION > to_version:
            raise ValueError(
                f"Passed {to_version=} is less "
                f"than min acceptable version value: {self.MIN_MIGRATION_VERSION}"
            )
        if from_version is not None and to_version < from_version:
            raise ValueError(f"Passed incompatible values from {from_version=} and {to_version=}")

        curr_version = self.migration_meta.check_migration_version()
        self.logger.info(f"Curr db version: {curr_version}")
        result = []
        if curr_version is None:    # if db not exists and we need just create it
            versions = list(range(self.MIN_MIGRATION_VERSION, to_version + 1))
            result = [
                (
                    version,
                    MigrationType.Up,
                )
                for version in versions
            ]
        elif is_drop:
            versions = list(range(self.MIN_MIGRATION_VERSION, to_version + 1))
            result = [
                (
                    version,
                    MigrationType.Up,
                )
                for version in versions
            ]
            result.insert(
                0,
                (0, MigrationType.Down)
            )
        elif from_version is None or (from_version is not None and curr_version <= from_version):
            # if from version is not specified or no need to downgrade to from version

            start_version = curr_version
            to_version = to_version

            if to_version <= curr_version:
                use_type = MigrationType.Down
                step = -1
            else:
                start_version += 1
                use_type = MigrationType.Up
                step = 1
                to_version += 1

            versions = list(range(start_version, to_version, step))
            result = [
                (
                    version,
                    use_type,
                )
                for version in versions
            ]
        elif from_version is not None:
            down_versions = list(range(curr_version, from_version, -1))
            down_migrations = [
                (
                    version,
                    MigrationType.Down,
                )
                for version in down_versions
            ]

            up_version = list(range(from_version + 1, to_version + 1))
            up_migrations = [
                (
                    version,
                    MigrationType.Up,
                )
                for version in up_version
            ]

            result = down_migrations + up_migrations
        else:
            self.logger.error(f"Reached unhandled if for: {is_drop=}; {from_version=}; {to_version=}")

        self.logger.info(f"Generate migration path: {result}")
        return result

    def _update_version_for_migration(self, migration: ExecMigration):
        if migration in self.NOT_TRACK_IN_META:
            self.logger.info(f"Received trackable migration: {migration}")
            return

        version = migration[0]

        if migration[1] == MigrationType.Down:
            version -= 1

        self.migration_meta.update_migration_version(version, self.shared_target_conn)

    # @retry(tries=3, delay=10, backoff=2, logger=RETRY_LOGGER)
    def sync(self, migration_path: List[ExecMigration]):
        self.logger.info(f"Start db sync with path: {len(migration_path)}")

        for migration in migration_path:
            migration_version = migration[0]
            migration_type = migration[1]

            if migration_version not in self.migration_files_map:
                self.logger.error(f"Received version: {migration_version} without any migration files.")
                self.logger.warning(f"Stop migration syncing.")
                return

            migration_file = self.migration_files_map[migration_version]

            self.logger.info(f"Run {migration_type.value} from {migration_version}_{migration_file.name}")

            migration_script = (
                migration_file.up_query
                if migration_type == MigrationType.Up
                else migration_file.down_query
            )

            if migration_version in self.DB_LEVEL_MIGRATIONS:
                self._execute_db_manage_query(migration_script)
                self._update_version_for_migration(migration)
                continue

            self._execute_migration_query(migration, migration_script)
