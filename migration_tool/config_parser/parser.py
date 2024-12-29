from pathlib import Path
from typing import List

from migration_tool.config_parser.sources import prepare_source
from migration_tool.config_parser.targets import prepare_target
from migration_tool.logger.mix_in import LoggerMixIn
import yaml


ROOT = Path(__file__).parent.parent.parent


class MigrationsConfigParser(LoggerMixIn):
    SOURCES = 'sources'
    TARGET = 'db'

    def __init__(self, config_path: str):
        self._config_path = ROOT / config_path

        self.logger.debug(f"Reading config file: {self._config_path}")

        with open(self._config_path, 'r') as file:
            self._raw_config = yaml.safe_load(file)

        if self.SOURCES not in self._raw_config or not isinstance(self._raw_config[self.SOURCES], List):
            raise ValueError(
                f"Can't read '{self.SOURCES}' config fragment list from config: {self._raw_config}"
            )

        if self.TARGET not in self._raw_config or not isinstance(self._raw_config[self.TARGET], List):
            raise ValueError(
                f"Can't read '{self.TARGET}' config fragment list from config: {self._raw_config}"
            )

        sources = {}

        for source_config in self._raw_config[self.SOURCES]:
            source_obj = prepare_source(source_config)

            if source_obj.id in sources:
                raise ValueError(
                    f"Received duplicates ids '{source_obj.id}' for sources"
                )

            sources[source_obj.id] = source_obj

        targets = {}

        for target_config in self._raw_config[self.TARGET]:
            target_object = prepare_target(target_config)

            if target_object.id in targets:
                raise ValueError(
                    f"Received duplicates ids '{target_object.id}' for tarrgets"
                )

            if target_object.source not in sources:
                raise ValueError(
                    f"Target DB: {target_object.id} used unknown source {target_object.source}"
                )

            targets[target_object.id] = target_object

        self.targets = targets
        self.sources = sources
