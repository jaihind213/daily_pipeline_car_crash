import configparser
import logging
import os
import re

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
    # Replace environment variable placeholders with actual values
    env_var_pattern = re.compile(r"^\$\{([^:}]+)(?::([^}]*))?\}$")

    # Iterate through all sections and keys
    for section in job_config.sections():
        for key in job_config[section]:
            value = job_config[section][key]
            match = env_var_pattern.match(value)
            if match:
                env_var_name = match.group(1)
                logging.info(
                    f"Replacing config parameter: {section}.{key} with environment variable: {env_var_name}"  # noqa: E501
                )
                default_value = match.group(2) if match.group(2) is not None else ""
                env_var_value = os.getenv(env_var_name, default_value)
                job_config[section][key] = env_var_value


def get_config():
    return job_config


def setup(config_file_path):
    load_config(config_file_path)
    configure_logging()
