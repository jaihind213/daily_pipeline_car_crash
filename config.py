import configparser
import logging
import os

job_config = configparser.SafeConfigParser()
logger = logging.getLogger(__name__)


def configure_logging():
    log_level = job_config.get("logging", "log_level", fallback="info")
    # set default loglevel
    logging.basicConfig(level=logging._nameToLevel[log_level.upper()])


def load_config(config_file_path):
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"config file {config_file_path} not found.")
    print("loading config from", config_file_path)
    with open(config_file_path) as cf:
        job_config.read_file(cf)


def get_config():
    return job_config


def setup(config_file_path):
    load_config(config_file_path)
    configure_logging()
