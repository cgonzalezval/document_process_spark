# -*- coding: utf-8 -*-
"""
Script to compute the most frequent words in all patents
"""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from azure.storage.blob import BlobServiceClient

from azure_utils import create_if_not_exists_container, get_account_url
from constants import FILTERED_STORAGE_NAME, FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER, \
    FREQUENT_WORDS_STORAGE_NAME, FREQUENT_WORDS_STORAGE_KEY, FREQUENT_WORDS_CONTAINER_NAME, \
    FREQUENT_WORDS_OUTPUT_FILE_NAME
from filter_english_patents import OUTPUT_COL_ENGLISH_TEXT
from launcher import logger
from utils import create_spark_session

LOGGER_CHILD_NAME = "FREQUENT_WORDS"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_MOST_FREQUENT_WORDS = 1000


def run_frequent_words(spark: SparkSession):
    logger.info("Starting execution")
    create_if_not_exists_container(storage_name=FREQUENT_WORDS_STORAGE_NAME, key=FREQUENT_WORDS_STORAGE_KEY,
                                   container_name=FREQUENT_WORDS_CONTAINER_NAME, logger=logger)
    df = read(spark)
    result_p = process(df)
    save(result_p)
    logger.info("Process finished!")
    return result_p


def read(spark: SparkSession) -> DataFrame:
    input_container = f"wasbs://{FILTERED_CONTAINER_NAME}@{FILTERED_STORAGE_NAME}.blob.core.windows.net"
    input_path = f"{input_container}/{FILTERED_OUTPUT_FOLDER}/"
    logger.info(f"Reading from: {input_path}")
    df = spark.read.parquet(input_path)
    return df


def process(df: DataFrame) -> pd.DataFrame:
    """Delete stop words and compute the most frequent words over the text of all patents"""
    df_clean = df.select('_file', OUTPUT_COL_ENGLISH_TEXT)
    tokenizer = Tokenizer(inputCol=OUTPUT_COL_ENGLISH_TEXT, outputCol="text_token")
    df_words_token = tokenizer.transform(df_clean)
    remover = StopWordsRemover(inputCol="text_token", outputCol="text_clean")
    df_words_no_stopw = remover.transform(df_words_token)
    # Stem and lemma?

    # Compute most frequent words
    counts = df_words_no_stopw.select(sf.explode("text_clean").alias("word"))
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
