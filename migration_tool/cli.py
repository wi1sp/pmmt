import logging
import os
import platform
import time

import argparse
from typing import Optional

from migration_tool.db_migration.postgresql import PostgreSQLMigrationRunner
from migration_tool.logger.mix_in import LoggerMixIn
from migration_tool.logger.utils import init_logger
from migration_tool.migration_config import MigrationConfig
from migration_tool.migration_files.loader.git_hub import FromGitHubRepoMigrationFilesLoader, \
    FromGitHubRepoMigrationFilesLoaderConfig
from migration_tool.settings import Settings

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
        Target name of migrated db. 
        ''',
    )
    parser.add_argument(
        "--type",
        type=str,
        required=True,
        dest='db_type',
        choices=[
            'postgresql'
        ],
        help='''
        Type of migrated db. 
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


BRANCH = 'master'
REPO_OWNER = 'book-hub-umsp'
REPO_NAME = 'book-hub-api'
MIGRATIONS_DIR = 'migrations'
logger = LoggerMixIn.init_logger()


def main(args):
    logger.info(f'CLI arguments: {args}')

    db_name = args.db_name
    db_type = args.db_type
    from_version = args.start_version
    is_drop = args.is_drop
    to_version = args.target_version

    settings = Settings(_env_file=os.environ.get('ENV_FILE', '.env.local'))

    config = MigrationConfig(db_name, db_type, settings)

    files_loader = FromGitHubRepoMigrationFilesLoader(FromGitHubRepoMigrationFilesLoaderConfig(
        branch=BRANCH,
        repo_name=REPO_NAME,
        repo_owner=REPO_OWNER,
        migration_files_dir=MIGRATIONS_DIR,
        github_pat_value=settings.GH_PAT,
    ))
    migration_runner = PostgreSQLMigrationRunner(
        config=config,
        files_loader=files_loader,
    )

    migration_path = migration_runner.build_migration_path(
        is_drop=is_drop,
        from_version=from_version,
        to_version=to_version,
    )

    migration_runner.sync(migration_path)


if __name__ == "__main__":
    main(parse_args())
