# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime


def create_logger(log_name, filename: str = None, log_folder: str = "./logs", log_level=logging.INFO,
                  log_format: str = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s") -> logging.Logger:
    """Init a logger"""
    logging.basicConfig(level=log_level, format=log_format)
    logger = logging.getLogger(log_name)
    formatter = logging.Formatter(log_format)

    if filename:
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        now = datetime.now().strftime("%Y-%m-%d_%H-%M.-%S")
        filename = f"{now}_{filename}.log"
        file_path = os.path.join(log_folder, filename)
        handler = logging.FileHandler(file_path)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = create_logger("TEXT-CLASSIFIER")
