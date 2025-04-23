import datetime
import os


def get_output_path(path_prefix, date: datetime.date):
    """
    Get the output path for the given date.
    :param path_prefix: The prefix for the output path.
    :param date: The date to use in the output path.
    :return: The output path.
    """
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    return os.path.join(
        path_prefix, f"year={year}", f"month={month}", f"day={day}"
    )  # noqa: E501
