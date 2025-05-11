import logging

SLT_LOGGER_NAME = 'sqllogictest'
logger = logging.getLogger(SLT_LOGGER_NAME)
log = logger

def loglevel(level):
    level = level.upper()
    if level == "NOTSET":
        return logging.NOTSET
    elif level == "DEBUG":
        return logging.DEBUG
    elif level == "INFO":
        return logging.INFO
    elif level == "WARNING":
        return logging.WARNING
    elif level == "ERROR":
        return logging.ERROR
    elif level == "CRITICAL":
        return logging.CRITICAL
