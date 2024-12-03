import dataclasses
from typing import Optional


@dataclasses.dataclass
class MigrationFile:
    version: int
    name: str
    up_query: str
    down_query: Optional[str]
