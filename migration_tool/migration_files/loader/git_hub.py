import dataclasses
import re
from typing import List, Dict, Any

from github import Github
from github.Auth import Token

from migration_tool.migration_files.loader.base import MigrationFilesLoader
from migration_tool.migration_files.file import MigrationFile


@dataclasses.dataclass(repr=False)
class FromGitHubRepoMigrationFilesLoaderConfig:
    branch: str
    repo_owner: str
    repo_name: str
    migration_files_dir: str
    github_pat_value: str


class FromGitHubRepoMigrationFilesLoader(MigrationFilesLoader):
    UP_MIGRATION_KEYWORD = 'up'
    DOWN_MIGRATION_KEYWORD = 'down'
    MIGRATION_FILE_REGEX = rf'^(\d+)_(.+)\.({UP_MIGRATION_KEYWORD}|{DOWN_MIGRATION_KEYWORD})\.(sql)$'
    BEGIN_COMMAND = 'BEGIN;'
    COMMIT_COMMAND = 'COMMIT;'

    def __init__(self, config: FromGitHubRepoMigrationFilesLoaderConfig):
        self._config = config

    @classmethod
    def _prepare_migration_file(cls, file: bytes):
        script = file.decode()
        script = script.rstrip()
        script = script.removeprefix(cls.BEGIN_COMMAND)
        script = script.removesuffix(cls.COMMIT_COMMAND)

        return script

    def load_files_list(self) -> List[MigrationFile]:
        self.logger.info(f"Connecting to git.hub repo: {self._config.repo_owner}/{self._config.repo_name}")
        with Github(auth=Token(self._config.github_pat_value)) as g:
            files = (
                g.get_organization(self._config.repo_owner).
                get_repo(self._config.repo_name).
                get_contents(path=self._config.migration_files_dir, ref=self._config.branch)
            )

        self.logger.info(f"Read file from github {self._config.migration_files_dir} count: {len(files)}")
        pattern = re.compile(self.MIGRATION_FILE_REGEX)

        migrations_data: Dict[int, Dict[str, Any]] = {}
        migration_names: Dict[int, str] = {}

        for file in files:
            match_result = pattern.match(file.name)
            if match_result is None:
                continue

            migration_version = int(match_result.group(1))
            migration_name = match_result.group(2)
            migration_data = file.decoded_content
            migration_type = match_result.group(3)

            if migration_version not in migration_names:
                migration_names[migration_version] = migration_name
                migrations_data[migration_version] = {}

            if migration_type in migrations_data[migration_version]:
                raise ValueError(
                    f"In migration for version: {migration_version} type: {migration_type} detected multiple files"
                )

            migrations_data[migration_version][migration_type] = self._prepare_migration_file(migration_data)

        result = []
        for version, name in migration_names.items():
            migration_data = migrations_data.get(version)
            if migrations_data is None:
                raise ValueError(f"No migration date for {version}_{name}")

            up = migration_data.get(self.UP_MIGRATION_KEYWORD)
            down = migration_data.get(self.DOWN_MIGRATION_KEYWORD)

            if up is None:
                raise ValueError(f"For migration file is required up migration existence")

            result.append(MigrationFile(
                version=version,
                name=name,
                up_query=up,
                down_query=down,
            ))

        result = list(sorted(result, key=lambda x: x.version))
        
        return result
