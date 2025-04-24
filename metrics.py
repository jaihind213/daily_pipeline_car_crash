import logging
from datetime import datetime

SUCCESS = "success"
FAILURE = "failure"


def report_job_stats(
    job_name: str,
    job_date: datetime,
    start_time: datetime,
    end_time: datetime,
    successful=False,
    metrics: dict = None,
):
    """
    Report job statistics
    :param successful: whether the job was successful or not
    :param job_name: Name of the job
    :param job_date: Date of the job
    :param start_time: Start time of the job
    :param end_time: End time of the job
    :param metrics: Additional metrics to report (k: metric_name, v: value)
    """
    time_sec = (end_time - start_time).total_seconds()
    status = SUCCESS if successful else FAILURE
    logging.info(
        "job %s for %s took %d seconds. status: %s",
        job_name,
        job_date,
        time_sec,
        status,
    )  # noqa: E501
    # todo: push db
