# -*- coding: utf-8 -*-
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StringType, Row
from logging import Logger

from azure_utils import create_if_not_exists_container


def flatten_df(df) -> DataFrame:
    df = df.select(flatten_schema(df.schema))
    return df


def flatten_schema(schema: DataFrame.schema, prefix=None):
    """Flattens StructFields into simple fields"""
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(sf.col(name).alias(name.replace(".", "_")))
    return fields


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Remove '.', ' ' and lower column names from a dataframe"""
    list_new_names = []
    for col in df.columns:
        new_name = col.strip()
        new_name = new_name.replace(" ", "")
        new_name = new_name.replace(".", "_")
        new_name = new_name.lower()
        list_new_names.append(new_name)
    df = df.toDF(*list_new_names)
    return df


def get_text_from_col(value):
    if value is None:
        return ""
    elif isinstance(value, list):  # ArrayType
        return " ".join([get_text_from_col(item) for item in value])
    elif isinstance(value, Row):  # StrucType. _<name> fields are xml attributes so they are omitted unless it is _VALUE
        return " ".join([get_text_from_col(value) for key, value in value.asDict().items() if
                         not (key.startswith("_") and key != "_VALUE")])
    elif isinstance(value, dict):  # MapType
        return " ".join([get_text_from_col(value) for _, value in value.items()])
    else:
        return str(value)


def save(spark: SparkSession, df: DataFrame, num_files: int, containter_name: str, storage_name: str,
         output_folder: str, logger:Logger, output_format="parquet"):
    """Saves the DataFrame into blob storage"""
    key = spark.conf.get(f"spark.hadoop.fs.azure.account.key.{storage_name}.blob.core.windows.net")
    create_if_not_exists_container(storage_name=storage_name, key=key, container_name=containter_name, logger=logger)

    output_container_path = f"wasbs://{containter_name}@{storage_name}.blob.core.windows.net"
    output_blob_folder = f"{output_container_path}/{output_folder}/"
    logger.info(f"Saving data into {output_blob_folder}")
    df.coalesce(num_files).write.format(output_format).mode("overwrite").parquet(output_blob_folder)
    logger.info(f"Data saved!")


udf_get_text_from_col = sf.udf(lambda x: get_text_from_col(x), StringType())
