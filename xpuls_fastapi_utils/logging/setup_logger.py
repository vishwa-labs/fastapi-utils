import logging
from logging.config import dictConfig

from xpuls_fastapi_utils.logging.fastapi_log_config import FALogConfig
dictConfig(FALogConfig().dict())
logger = logging.getLogger("default")
