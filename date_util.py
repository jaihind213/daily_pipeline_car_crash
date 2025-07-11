import pytz
from datetime import datetime, timedelta


def get_default_job_date(tz="America/Chicago") -> datetime.date:
    """
    Get the default job date for the given timezone.
    :param tz: timezone
    :return: date
    """
    # Current time in UTC
    utc_now = datetime.now(pytz.UTC)

    # Convert to the given timezone
    local_time = utc_now.astimezone(pytz.timezone(tz))

    # Subtract 1 day from the local date
    job_date = (local_time - timedelta(days=1)).date()

    return job_date


def get_job_date(job_args, config) -> datetime.date:
    date_str = get_default_job_date(
        config.get("job", "target_timezone", fallback="America/Chicago")
    ).strftime("%Y-%m-%d")
    if len(job_args) > 2:
        date_str = job_args[2]
    return datetime.strptime(date_str, "%Y-%m-%d").date()
