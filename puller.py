import logging
import os
import time

import pandas as pd
from sodapy import Socrata
from tabulate import tabulate

debug_dataframes = (
    os.environ.get("DEBUG_DATAFRAMES", "false").lower() == "true"
)  # noqa: E501


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
                    select="*",
                )
                df = pd.DataFrame.from_records(results)
                if df.empty:
                    break
                df.to_parquet(
                    output_dir_path + f"output{idx}.parquet",
                    engine="pyarrow",
                    index=True,
                    compression="snappy",
                )
                recs_fetched += len(df)
                if debug_dataframes:
                    df[f"ds_{dataset_id}_{idx}"] = None
                    print(
                        tabulate(
                            df.head(20),
                            headers="keys",
                            tablefmt="psql",
                        )
                    )
                idx += 1
                offset += batch_size
                logging.info(
                    f"fetched {len(df)} records from offset {offset} for dataset_id: {dataset_id} for {for_year}-{mm}-{dd} ..."  # noqa: E501
                )
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
