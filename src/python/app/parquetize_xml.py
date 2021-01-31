# -*- coding: utf-8 -*-
"""
Script to read the santized xmls and store the output as parquet files
"""
from pyspark.sql import DataFrame, SparkSession
from constants import SANITIZED_STORAGE_NAME, SANITIZED_CONTAINER_NAME, PARQUET_CONTAINER_NAME, PARQUET_STORAGE_NAME, \
    PARQUET_OUTPUT_FOLDER
from launcher import logger
from utils import create_spark_session

LOGGER_CHILD_NAME = "PARQUETIZER"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 116  # TODO parametrize from num files and size


def run_parquetizer(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark)
    result = process(df)
    save(df=result, num_files=NUM_OUTPUT_FILES)
    logger.info("Process finished!")


def read(spark: SparkSession) -> DataFrame:
    input_path = f"wasbs://{SANITIZED_CONTAINER_NAME}@{SANITIZED_STORAGE_NAME}.blob.core.windows.net"
    logger.info(f"Reading from: {input_path}")
    df = spark.read.format("com.databricks.spark.xml").option("rowTag", "questel-patent-document").option(
        "mode", "DROPMALFORMED").load(input_path)
    return df


def process(df: DataFrame) -> DataFrame:
    # TODO ¿flatten fields?
    return df


def save(df: DataFrame, num_files: int):
    output_container_path = f"wasbs://{PARQUET_CONTAINER_NAME}@{PARQUET_STORAGE_NAME}.blob.core.windows.net"
    output_blob_folder = f"{output_container_path}/{PARQUET_OUTPUT_FOLDER}/"
    logger.info(f"Saving data into {output_blob_folder}")
    df.coalesce(num_files).write.mode("overwrite").parquet(output_blob_folder)
    logger.info(f"Data saved!")


if __name__ == '__main__':
    spark_session = create_spark_session("parquetizer")
    run_parquetizer(spark_session)
