import abc
import dataclasses
from abc import ABC
from functools import lru_cache
from typing import List, Dict

from migration_tool.migration_files.file import MigrationFile
from migration_tool.migration_files.loader.base import MigrationFilesLoader
from migration_tool.migration_files.loader.git_hub import FromGitHubRepoMigrationFilesLoaderConfig, \
    FromGitHubRepoMigrationFilesLoader
from migration_tool.settings import settings


@dataclasses.dataclass
class MigrationFilesSource(ABC):
    id: str
    type: str

    @abc.abstractmethod
    def get_loader(self) -> MigrationFilesLoader:
        raise NotImplementedError()


TYPE_KEYWORD = 'type'


@dataclasses.dataclass
class GitHubMigrationsFileSource(MigrationFilesSource):
    TYPE_NAME = 'github'

    branch: str
    repo: str
    repo_owner: str
    path: str

    def get_loader(self) -> MigrationFilesLoader:
        pat_name = f"{self.id}_PAT".lower()

        pat_value = getattr(settings, pat_name)
        if pat_value is None:
            raise ValueError(
                f"For config: {self.id} is env variable {pat_name} is required"
            )

        loader_config = FromGitHubRepoMigrationFilesLoaderConfig(
            branch=self.branch,
            repo_name=self.repo,
            repo_owner=self.repo_owner,
            migration_files_dir=self.path,
            github_pat_value=pat_value,
        )
        loader = FromGitHubRepoMigrationFilesLoader(loader_config)

        return loader


def prepare_for_github(config: Dict) -> MigrationFilesSource:
    if TYPE_KEYWORD not in config or config[TYPE_KEYWORD] != GitHubMigrationsFileSource.TYPE_NAME:
        raise ValueError(
            f"From given config: {config} can't find correct type name: {GitHubMigrationsFileSource.TYPE_NAME}"
        )

    source = GitHubMigrationsFileSource(
        id=config['id'],
        type=config['type'],
        branch=config['branch'],
        repo=config['repo'],
        repo_owner=config['repo_owner'],
        path=config['path'],
    )

    return source


def prepare_source(config: Dict) -> MigrationFilesSource:
    type_name = config.get(TYPE_KEYWORD)
    source_map = {
        GitHubMigrationsFileSource.TYPE_NAME: prepare_for_github,
    }

    if type_name is None:
        raise ValueError(
            f"Source config does not have config type keyword '{TYPE_KEYWORD}'"
        )

    if type_name not in source_map:
        raise ValueError(
            f"Given config type '{type_name}' not have source builder in map: {source_map}"
        )

    return source_map[type_name](config)
