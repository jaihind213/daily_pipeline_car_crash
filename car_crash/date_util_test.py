import configparser
from datetime import datetime, timedelta

import pytz

from car_crash.date_util import get_default_job_date, get_job_date


def test_get_default_job_date():
    """
    Test the get_default_job_date function.
    """
    # Test with default timezone
    assert (
        get_default_job_date()
        == (
            datetime.now(pytz.timezone("America/Chicago")) - timedelta(days=1)
        ).date()  # noqa: E501
    ), "Job date is not correct"

    # Test with a different timezone
    assert (
        get_default_job_date(tz="UTC")
        == (datetime.now(pytz.UTC) - timedelta(days=1)).date()
    ), "Job date is not correct"


def test_get_job_date():
    """
    Test the get_job_date function.
    """
    job_config = configparser.SafeConfigParser()
    job_config.add_section("job")
    job_config.set("job", "target_timezone", "America/Chicago")

    # Test with default parameters
    assert (
        get_job_date(["pull_data_job.py", "config.ini"], job_config)
        == (
            datetime.now(pytz.timezone("America/Chicago")) - timedelta(days=1)
        ).date()  # noqa: E501
    ), "Job date is not correct"

    # Test with a different date
    assert (
        get_job_date(
            ["pull_data_job.py", "config.ini", "2023-10-02"], job_config
        )  # noqa: E501
        == datetime.strptime("2023-10-02", "%Y-%m-%d").date()
    ), "Job date is not correct"
