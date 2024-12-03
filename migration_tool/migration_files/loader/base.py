from abc import ABC, abstractmethod
from typing import List, Dict

from migration_tool.logger.mix_in import LoggerMixIn
from migration_tool.migration_files.file import MigrationFile


class MigrationFilesLoader(LoggerMixIn, ABC):
    @abstractmethod
    def load_files_list(self) -> List[MigrationFile]:
        raise NotImplementedError()
