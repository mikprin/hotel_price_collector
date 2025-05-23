import logging


def setup_logger(name: str, log_file: str | None = None, level: int = logging.DEBUG) -> logging.Logger:
    """
    Set up a logger with a specific name and log file.
    
    Args:
        name (str): The name of the logger.
        log_file (str): The file where logs will be written.
        level (int): The logging level (default is INFO).
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    stdout_handler = logging.StreamHandler()
    handlers = [stdout_handler]
    logger = logging.getLogger(name)
    if log_file is not None:
        file_handler = logging.FileHandler(log_file)
        handlers.append(file_handler)
    
    for handler in handlers:
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    logger.addHandler(stdout_handler)
    if log_file is not None:
        logger.addHandler(file_handler)
    
    logger.setLevel(level)
    
    return logger

general_logger = setup_logger('general_logger')