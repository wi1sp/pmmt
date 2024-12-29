import dataclasses

from migration_tool.db_types import DBType


@dataclasses.dataclass
class MigrationConfig:
    db_name: str
    db_type: DBType
    db_user: str
    db_pass: str
    db_port: str
    db_host: str
