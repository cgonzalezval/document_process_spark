# -*- coding: utf-8 -*-
"""
Script to compute the most frequent words in all patents. It reads from a text in lowercase and without stopwords
"""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from azure.storage.blob import BlobServiceClient

from azure_utils import get_account_url
from constants import PROCESSED_TEXT_STORAGE_NAME, PROCESSED_TEXT_CONTAINER_NAME, PROCESSED_TEXT_OUTPUT_FOLDER, \
    FREQUENT_WORDS_STORAGE_NAME, FREQUENT_WORDS_STORAGE_KEY, FREQUENT_WORDS_CONTAINER_NAME, \
    FREQUENT_WORDS_OUTPUT_FILE_NAME
from filter_english_patents import OUTPUT_COL_ENGLISH_TEXT
from launcher import logger
from utils import create_spark_session
from spark_utils import read

LOGGER_CHILD_NAME = "FREQUENT_WORDS"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_MOST_FREQUENT_WORDS = 1000


def run_frequent_words(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark=spark, storage_name=PROCESSED_TEXT_STORAGE_NAME, containter_name=PROCESSED_TEXT_CONTAINER_NAME,
              output_folder=PROCESSED_TEXT_OUTPUT_FOLDER, logger=logger)

    result_p = process(df)

    save(result_p)
    logger.info("Process finished!")
    return result_p


def process(df: DataFrame) -> pd.DataFrame:
    """Delete stop words and compute the most frequent words over the text of all patents"""
    counts = df.select(sf.explode_outer(f"{OUTPUT_COL_ENGLISH_TEXT}_stopwords").alias("word"))
    # TODO check num partitions
    result = counts.groupBy("word").count()
    result = result.sort(sf.col("count").desc()).limit(NUM_MOST_FREQUENT_WORDS)
    result_p = result.toPandas()
    return result_p


def save(df_p: pd.DataFrame):
    logger.info(f"Saving local data into {FREQUENT_WORDS_OUTPUT_FILE_NAME}")
    assert FREQUENT_WORDS_OUTPUT_FILE_NAME.endswith(".csv")
    df_p.to_csv(FREQUENT_WORDS_OUTPUT_FILE_NAME, header=True, index=False, sep=",", encoding="utf-8")

    logger.info(f"Uploading data...")
    output_url = get_account_url(FREQUENT_WORDS_STORAGE_NAME)
    output_service = BlobServiceClient(account_url=output_url, credential=FREQUENT_WORDS_STORAGE_KEY)
    output_container = output_service.get_container_client(FREQUENT_WORDS_CONTAINER_NAME)
    upload_blob_client = output_container.get_blob_client(FREQUENT_WORDS_OUTPUT_FILE_NAME)
    with open(FREQUENT_WORDS_OUTPUT_FILE_NAME, "rb") as data:
        upload_blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
    logger.info("Upload completed!")


if __name__ == '__main__':
    spark_session = create_spark_session("frequent_words")
    run_frequent_words(spark_session)
