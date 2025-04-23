import json
import logging
import os
import sys

import puller
from config import get_config, setup
from date_util import get_job_date
from utilz import get_output_path

logger = logging.getLogger("pull_data")


if __name__ == "__main__":
    # look in project dir for defaults.
    if len(sys.argv) < 1:
        logger.error(
            "usage: python pull_data_job.py <config_file_path> <yyyy-mm-dd> "
        )  # noqa: E501
        sys.exit(1)

    config_file_path = sys.argv[1]
    setup(config_file_path)

    date_to_pull = get_job_date(sys.argv, get_config())

    logger.info("pulling data for date: %s", date_to_pull)
    logger.info("using config file: %s", config_file_path)

    # params
    datasets_map = json.loads(get_config()["pull_job"]["datasets"])
    output_path_prefix = get_config()["pull_job"]["raw_data_path"]
    sleep_time_millis = int(
        get_config().get("pull_job", "sleep_time_millis", fallback="100")
    )
    batch_size = int(get_config().get("pull_job", "batch_size", fallback="2000"))  # noqa: E501
    timeout_sec = int(get_config().get("pull_job", "timeout_sec", fallback="10"))  # noqa: E501
    domain = get_config().get(
        "pull_job", "domain", fallback="data.cityofchicago.org"
    )  # noqa: E501

    # do the pull
    for name, config in datasets_map.items():
        id = config["id"]
        time_column = config["time_column"]
        path_to_save = get_output_path(
            output_path_prefix + "/" + name, date_to_pull
        )  # noqa: E501
        logger.info(
            "pulling dataset name: %s, id: %s, path: %s",
            name,
            id,
            path_to_save,  # noqa: E501
        )
        puller.pull_chicago_dataset(
            id,
            date_to_pull.year,
            date_to_pull.month,
            date_to_pull.day,
            time_column,
            path_to_save,
            app_token=os.environ.get("SOCRATA_APP_TOKEN"),
            username=os.environ.get("SOCRATA_USERNAME"),
            password=os.environ.get("SOCRATA_PASSWORD"),
            timeout_sec=timeout_sec,
            batch_size=batch_size,
            sleep_time_millis=sleep_time_millis,
            domain=domain,
        )
