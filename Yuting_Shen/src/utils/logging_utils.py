"""
Utility module for setting up logging across the pipeline.
"""

import logging
import os
from datetime import datetime


def setup_logger(log_file=None, log_level=logging.INFO):
    """
    Set up logger with file and console handlers.

    Args:
        log_file (str, optional): Path to log file
        log_level (int): Logging level

    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create file handler if log file is specified
    if log_file:
        # Ensure directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_pipeline_start(logger, pipeline_name, **kwargs):
    """
    Log pipeline start with parameters.

    Args:
        logger (logging.Logger): Logger
        pipeline_name (str): Name of the pipeline
        **kwargs: Additional parameters to log
    """
    logger.info(f"Starting {pipeline_name} pipeline")
    for key, value in kwargs.items():
        logger.info(f"- {key}: {value}")


def log_pipeline_end(logger, pipeline_name, status, duration, **results):
    """
    Log pipeline end with results.

    Args:
        logger (logging.Logger): Logger
        pipeline_name (str): Name of the pipeline
        status (str): Pipeline status (success/error)
        duration (float): Duration in seconds
        **results: Additional results to log
    """
    logger.info(f"Finished {pipeline_name} pipeline")
    logger.info(f"- Status: {status}")
    logger.info(f"- Duration: {duration:.2f} seconds")

    for key, value in results.items():
        logger.info(f"- {key}: {value}")