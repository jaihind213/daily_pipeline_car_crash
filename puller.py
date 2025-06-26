import logging
import os
import shutil
import time
from datetime import datetime

import pandas as pd
import pytz
from sodapy import Socrata

from utilz import debug_pandas_df

os.environ["TZ"] = "GMT"


def __write_to_parquet(pandas_df, path, file_idx):
    if path.startswith("/") or path.startswith("file://"):  # noqa: E501
        if file_idx == 0:
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)

    now = datetime.now(pytz.timezone("UTC"))
    pandas_df.to_parquet(
        path + f"/output{file_idx}_{now.timestamp()}.parquet",
        engine="pyarrow",
        index=True,
        compression="snappy",
    )


def pull_chicago_dataset(
    dataset_id,
    for_year: int,
    for_month: int,
    for_day: int,
    time_filter_column: str,
    output_dir_path: str,
    app_token=None,
    username=None,
    password=None,
    timeout_sec=10,
    batch_size=2000,
    sleep_time_millis=100,
    domain="data.cityofchicago.org",
    columns=None,
) -> int:
    """
    Pulls a dataset from the Chicago data portal using the Socrata API.
    :param sleep_time_millis: time between api calls (so that we don't overload the api)
    :param output_dir_path: output path to save the parquet files
    :param domain:  ex: 'data.cityofchicago.org'
    :param dataset_id: ex: '85ca-t3if' from https://dev.socrata.com/foundry/data.cityofchicago.org/85ca-t3if  # noqa: E501
    :param for_year:
    :param for_month:
    :param for_day:
    :param app_token:
    :param username:
    :param password:
    :param timeout_sec:
    :param batch_size:
    :param time_filter_column: The timestamp column to filter on, e.g., 'crash_date'  # noqa: E501
    :params columns: list of columns to fetch from the dataset
    :return: number of records fetched
    """

    offset = 0
    recs_fetched = 0
    mm = str(for_month).zfill(2)
    dd = str(for_day).zfill(2)
    where_clause = f"{time_filter_column} >= '{for_year}-{mm}-{dd}T00:00:00' and {time_filter_column} <= '{for_year}-{mm}-{dd}T23:59:59'"  # noqa: E501
    logging.info(
        f"where clause: {where_clause} for {for_year}-{mm}-{dd} for dataset_id: {dataset_id} ..."  # noqa: E501
    )
    select_columns = (
        "*" if not columns else ",".join(set(list(columns))).upper()
    )  # noqa: E501

    idx = 0
    with Socrata(
        domain,
        app_token,
        username=username,
        password=password,
        timeout=timeout_sec,  # noqa: E501
    ) as client:
        while True:
            try:
                results = client.get(
                    dataset_id,
                    offset=offset,
                    limit=batch_size,
                    where=where_clause,
                    select=select_columns,
                )
                df = pd.DataFrame.from_records(results)
                if df.empty:
                    break

                recs_fetched += len(df)
                debug_pandas_df(df, f"pulled_dataset_{dataset_id}_idx_{idx}")

                __write_to_parquet(df, output_dir_path, idx)

                idx += 1
                logging.info(
                    f"fetched {len(df)} records from offset {offset} for dataset_id: {dataset_id} for {for_year}-{mm}-{dd} ..."  # noqa: E501
                )
                offset += batch_size
                # we are pulling from public dataset,
                # so we need to sleep for a while to not overload the api
                time.sleep(sleep_time_millis / 1000)
            except Exception as e:
                logging.error(
                    f"error fetching data for dataset_id {dataset_id},for {for_year}-{mm}-{dd}: {e}"  # noqa: E501
                )
                raise e
    print(
        f"fetched {recs_fetched} records for dataset_id {dataset_id} for {for_year}-{mm}-{dd}"  # noqa: E501
    )
    return recs_fetched
