import logging
from util.appconfigreader import AppConfigReader

# Read logging config
appConfigReader = AppConfigReader()
if 'LOGS' in appConfigReader.config:
    LOG_CFG = appConfigReader.config['LOGS']
    LOG_LEVEL = LOG_CFG['LOG_LEVEL'].upper()
    LOG_FILE = LOG_CFG['LOG_FILE']
    ERR_FILE = LOG_CFG['ERR_FILE']
else:
    LOG_LEVEL = "INFO".upper()
    LOG_FILE = "E:\\Spark\\logs\\irisflowerclassification.log"
    ERR_FILE = "E:\\Spark\\logs\\irisflowerclassification.err"
# Logging format
formatter = logging.Formatter('%(asctime)s: %(name)s: %(levelname)s: %(message)s')


def getAppLogger(logger_name):
    """Defines the logger and returns the same"""
    # Create a logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)

    # Create a handler for the level INFO
    log_handler = logging.FileHandler(LOG_FILE)
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    # Create a handler for errors
    error_handler = logging.FileHandler(ERR_FILE)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    # If debug is enabled, create a stream handler
    if logger.getEffectiveLevel() == logging.DEBUG:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
