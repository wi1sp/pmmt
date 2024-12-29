import abc
import dataclasses
from typing import Dict

from migration_tool.db_migration.base import DBMigrationRunner
from migration_tool.db_migration.postgresql import PostgreSQLMigrationRunner
from migration_tool.db_types import DBType
from migration_tool.migration_config import MigrationConfig
from migration_tool.migration_files.loader.base import MigrationFilesLoader
from migration_tool.settings import settings


@dataclasses.dataclass
class TargetDB:
    id: str
    type: str
    name: str
    source: str

    @abc.abstractmethod
    def get_runner(self, loader: MigrationFilesLoader) -> DBMigrationRunner:
        raise NotImplementedError()


@dataclasses.dataclass
class TargetPSQLDB(TargetDB):
    def get_runner(self, loader: MigrationFilesLoader) -> DBMigrationRunner:
        env_mapping = {
            'db_user': f"{self.id}_USER",
            'db_pass': f"{self.id}_USER_PASSWORD",
            'db_port': f"{self.id}_PORT",
            'db_host': f"{self.id}_HOST",
        }

        args = {
            k: getattr(settings, v.lower())
            for k, v in env_mapping.items()
        }
        args['db_type'] = DBType(self.type)
        args['db_name'] = self.name

        config = MigrationConfig(
            **args,
        )

        runner = PostgreSQLMigrationRunner(
            config=config,
            files_loader=loader
        )

        return runner


def prepare_target(config: Dict) -> TargetDB:
    # todo  mapping for different types
    return TargetPSQLDB(
        id=config['id'],
        type=config['type'],
        source=config['source'],
        name=config['name'],
    )
