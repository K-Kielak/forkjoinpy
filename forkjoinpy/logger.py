import logging


LOGGER_NAME = "ForkJoinPoolLogger"
LOG_LEVEL = 10
_logger = None


def get_logger() -> logging.Logger:
    global _logger
    if _logger is None:
        console_handler = create_console_handler()
        _logger = logging.getLogger(LOGGER_NAME)
        _logger.setLevel(LOG_LEVEL)
        _logger.addHandler(console_handler)

    return _logger


def create_console_handler() -> logging.StreamHandler:
    message_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_formatter = logging.Formatter(fmt=message_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(LOG_LEVEL)
    return console_handler
