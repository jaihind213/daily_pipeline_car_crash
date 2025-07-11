import duckdb
import os
import shutil

from puller import pull_chicago_dataset

os.environ["TZ"] = "GMT"


def test_pull_chicago_dataset():
    """
    Test the pull_dataset function.
    """
    data_dir = "/tmp/pull_data/"

    if os.path.isdir(data_dir):
        shutil.rmtree(data_dir)
    else:
        os.makedirs(data_dir, exist_ok=True)

    fetched = pull_chicago_dataset(
        domain="data.cityofchicago.org",
        dataset_id="85ca-t3if",
        for_year=2024,
        for_month=4,
        for_day=20,
        time_filter_column="crash_date",
        output_dir_path=data_dir,
        app_token=os.environ.get("SOCRATA_APP_TOKEN"),
        username=None,
        password=None,
        timeout_sec=10,
        batch_size=101,
        sleep_time_millis=100,
    )
    # pull again lets see if overwritten
    fetched = pull_chicago_dataset(
        domain="data.cityofchicago.org",
        dataset_id="85ca-t3if",
        for_year=2024,
        for_month=4,
        for_day=20,
        time_filter_column="crash_date",
        output_dir_path=data_dir,
        app_token=os.environ.get("SOCRATA_APP_TOKEN"),
        username=None,
        password=None,
        timeout_sec=10,
        batch_size=101,
        sleep_time_millis=100,
    )

    # Check if the number of records fetched is greater than zero
    assert fetched == 283, "fetched records count is not as expected"

    # Direct query from Parquet file
    parquet_count = duckdb.query(
        "SELECT COUNT(*) FROM '/tmp/pull_data/*.parquet'"
    ).fetchone()[0]

    distinct_date_seen = duckdb.query(
        "SELECT count(DISTINCT CAST(crash_date AS DATE))  FROM '/tmp/pull_data/*.parquet'"  # noqa: E501
    ).fetchone()[0]
    assert distinct_date_seen == 1, "distinct date seen should be 1"

    assert (
        parquet_count == fetched
    ), "records found in the Parquet file does not match the fetched count"

    # check number of batches i.e. offset limit logic
    num_files = len(
        [
            f
            for f in os.listdir(data_dir)
            if os.path.isfile(os.path.join(data_dir, f))  # noqa: E501
        ]
    )
    assert num_files == 3, "number of batches is not as expected"
