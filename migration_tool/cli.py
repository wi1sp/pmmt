import logging
import os
import platform
import time

import argparse
from typing import Optional

from migration_tool.config_parser.parser import MigrationsConfigParser
from migration_tool.db_migration.base import DBMigrationRunner
from migration_tool.db_migration.postgresql import PostgreSQLMigrationRunner
from migration_tool.logger.mix_in import LoggerMixIn
from migration_tool.logger.utils import init_logger
from migration_tool.migration_config import MigrationConfig
from migration_tool.migration_files.loader.git_hub import FromGitHubRepoMigrationFilesLoader, \
    FromGitHubRepoMigrationFilesLoaderConfig
from migration_tool.settings import settings

PROG = 'cli'
init_logger()
# getting only important messages from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)

if not platform.system() == 'Windows':
    os.environ['TZ'] = 'Europe/Moscow'
    getattr(time, 'tzset')()


def version_value(value: Optional[str]):
    if value is None:
        return None

    value = int(value)
    if value < 0:
        raise ValueError(f"Version can't have negative value")

    return value


def parse_args():
    # create parser object
    parser = argparse.ArgumentParser(
        prog=PROG,
        description='Run migration for db.'
    )
    parser.add_argument(
        "--name",
        type=str,
        required=True,
        dest='db_name',
        help='''
        Target id from config file. 
        ''',
    )
    parser.add_argument(
        "--from",
        type=version_value,
        default=None,
        dest='start_version',
        help='''
        Migration start version. If 'none' - for start version used current db version.
        ''',
    )
    parser.add_argument(
        "--to",
        type=version_value,
        required=True,
        default=0,
        dest='target_version',
        help='''
        Migration target version.
        ''',
    )
    parser.add_argument(
        "--drop",
        action='store_true',
        dest='is_drop',
        help='''
        Flag for force db re-initialization.
        '''
    )
    parser.add_argument(
        "--mock1",
        action='store_true',
        help='''
        Mock param for bypassing some limitations.
        '''
    )
    parser.add_argument(
        "--mock2",
        action='store_true',
        help='''
        Mock param for bypassing some limitations.
        '''
    )
    return parser.parse_args()


logger = LoggerMixIn.init_logger()


def get_runner_for_db(name: str, parser: MigrationsConfigParser) -> DBMigrationRunner:
    target = parser.targets.get(name, None)

    if target is None:
        raise ValueError(
            f"Given DB name not present in config {settings.CONFIG_PATH}"
        )

    source = parser.sources.get(
        target.source,
        None,
    )

    if source is None:
        raise ValueError(
            f"Given migration source not present in config {settings.CONFIG_PATH}"
        )

    loader = source.get_loader()
    runner = target.get_runner(loader)

    return runner


def main(args):
    logger.info(f'CLI arguments: {args}')

    db_name = args.db_name
    from_version = args.start_version
    is_drop = args.is_drop
    to_version = args.target_version

    parser = MigrationsConfigParser(
        config_path=settings.CONFIG_PATH,
    )

    migration_runner = get_runner_for_db(db_name, parser)

    migration_path = migration_runner.build_migration_path(
        is_drop=is_drop,
        from_version=from_version,
        to_version=to_version,
    )

    migration_runner.sync(
        migration_path,
    )


if __name__ == "__main__":
    main(parse_args())
