import logging
import sys

def setup_logger(name="app_logger", log_file="app.log", level=logging.DEBUG):
    logger = logging.getLogger(name)

    if not logger.handlers:
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.DEBUG)

        logger.setLevel(level)

        # Define a formatter that includes module & logger name
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(module)s - %(levelname)s - %(message)s"
        )

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Capture uncaught exceptions
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            logger.error("Uncaught exception",
                         exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = handle_exception

    return logger
