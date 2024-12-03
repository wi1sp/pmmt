from functools import cached_property
from logging import Logger, getLogger
from typing import Optional


class LoggerMixIn:
    ROOT = 'pmmt'

    @classmethod
    def init_logger(cls, name: Optional[str] = None) -> Logger:
        """
        Initialize logger with specific name.
        Parameters:
            name (str): name of logger
        Returns:
            logger (Logger): new logger with given name.
        """
        logger = getLogger(f"{cls.ROOT}.{name}" if name is not None else cls.ROOT)
        return logger

    @cached_property
    def logger(self):
        """
        Class level logger.
        """
        return self.init_logger(self.__class__.__module__)
