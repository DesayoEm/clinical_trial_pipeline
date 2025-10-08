import logging
import os
from logging.handlers import TimedRotatingFileHandler



def setup_logging(log_type: str):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, f"logs/{log_type}")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger('ct_pipeline')
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        file_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, 'app.log'),
            when='midnight',
            interval=1,
            backupCount=7,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    return logger


logger = setup_logging('general')
error_logger = setup_logging('errors')
progress_logger = setup_logging('progress')

