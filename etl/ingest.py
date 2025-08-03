import logging
from datetime import datetime

from pyspark.sql import functions as F

from etl import tables
from etl.transform import transform
from etl.utilz import get_output_path


def ingest_to_iceberg(
    spark,
    raw_data_path,
    job_date: datetime.date,
    catalog,
    db,
    table_name,
    data_timezone="America/Chicago",
    transformer_name=None,
):
    path_to_read = get_output_path(raw_data_path + "/" + table_name, job_date)
    logging.info("reading from %s", path_to_read)

    # fully qualified table name
    fqtn = f"{catalog}.{db}.{table_name}"
    if not spark.catalog.tableExists(fqtn):
        ddl = tables.tables_ddl[table_name]
        ddl = ddl % (catalog, db)
        logging.info("creating table %s ....", fqtn)
        spark.sql(ddl.lower())

    # apply transform
    df = spark.read.parquet(path_to_read)
    if transformer_name is not None:
        logging.info("transforming data using %s", transformer_name)
        df_meta = spark.table(fqtn)
        schema_dict = {
            field.name: field.dataType.simpleString()
            for field in df_meta.schema.fields  # noqa: E501
        }
        df = transform(transformer_name, df, schema_dict, data_timezone)
    else:
        logging.warning("not transforming data ... for table %s", fqtn)

    iceberg_table = spark.table(fqtn)
    expected_cols = iceberg_table.columns

    # Add missing columns
    for col in expected_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    # Select columns in the right order
    df = df.select(*expected_cols)

    # Write to Iceberg
    logging.info("ingesting to iceberg table %s", fqtn)
    df.writeTo(fqtn).overwritePartitions()
