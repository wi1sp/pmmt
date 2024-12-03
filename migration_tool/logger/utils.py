import logging
import logging.handlers
import logging.config
import json
import os
import datetime
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """
    formatter class for logger that formats output into json string
    """
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    # @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields['stack_info'] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        return message


def init_logger():
    # might add log_dir: str = './debug'
    """
    Initializing root logger with configs from file

    Args:
    None

    Returns:
    None
    """
    # creating directory for logs
    Path('.').mkdir(parents=True, exist_ok=True)
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')

    with open(config_path, "r", encoding='utf8') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
