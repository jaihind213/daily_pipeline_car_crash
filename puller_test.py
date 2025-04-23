import os
import shutil

import duckdb

from puller import pull_chicago_dataset


def test_pull_chicago_dataset():
    """
    Test the pull_dataset function.
    """
    data_dir = "/tmp/pull_data/"
    shutil.rmtree(data_dir)
    os.mkdir(data_dir)
    fetched = pull_chicago_dataset(
        domain="data.cityofchicago.org",
        dataset_id="85ca-t3if",
        for_year=2025,
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
    assert fetched == 202, "fetched records count is not as expected"

    # Direct query from Parquet file
    parquet_count = duckdb.query(
        "SELECT COUNT(*) FROM '/tmp/pull_data/*.parquet'"
    ).fetchone()[0]
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
    assert num_files == 2, "number of batches is not as expected"
