# -*- coding: utf-8 -*-
"""
Script to identify clusters in the energy patents.
We are going to train different lda models and checks its results
"""
import sys
import pandas as pd
from typing import List
from pyspark import StorageLevel
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
from azure.storage.blob import BlobServiceClient

from azure_utils import create_if_not_exists_container, get_account_url
from constants import ENERGY_PATENTS_STORAGE_NAME, ENERGY_PATENTS_CONTAINER_NAME, \
    ENERGY_PATENTS_OUTPUT_FOLDER, TOPIC_CLUSTERING_STORAGE_NAME, TOPIC_CLUSTERING_CONTAINER_NAME, \
    TOPIC_CLUSTERING_OUTPUT_LDA, TOPIC_CLUSTERING_OUTPUT_CV, TOPIC_CLUSTERING_OUTPUT_LDA_RESULT_PREFIX
from launcher import logger
from utils import create_spark_session
from spark_utils import read

LOGGER_CHILD_NAME = "ENERGY_CLUSTERING"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 200


def run_energy_clustering(spark: SparkSession, list_num_topics: List[int]) -> pd.DataFrame:
    logger.info("Starting execution")
    df = read(spark=spark, storage_name=ENERGY_PATENTS_STORAGE_NAME, containter_name=ENERGY_PATENTS_CONTAINER_NAME,
              output_folder=ENERGY_PATENTS_OUTPUT_FOLDER, logger=logger)

    min_df = 0.05
    df = df.select("_file", "prediction", "english_text_features")
    df = df.filter(sf.col("prediction") == 1)
    cv = CountVectorizer(inputCol="english_text_features", outputCol="features", minDF=min_df)
    cv_model = cv.fit(df)
    df_vectorized = cv_model.transform(df)
    df_vectorized.persist(StorageLevel.DISK_ONLY)
    logger.info(f"Vocabulary size: {len(cv_model.vocabulary)}")
    save_ml_model(spark=spark, model=cv_model, storage_name=TOPIC_CLUSTERING_STORAGE_NAME,
                  container_name=TOPIC_CLUSTERING_CONTAINER_NAME, output_folder=TOPIC_CLUSTERING_OUTPUT_CV,
                  output_suffix=min_df)

    results_log_likelihood = []
    results_log_perplexity = []
    for n in list_num_topics:
        lda = LDA(k=n, maxIter=100, seed=18)
        model = lda.fit(df_vectorized)

        ll = model.logLikelihood(df_vectorized)
        lp = model.logPerplexity(df_vectorized)
        logger.info(f"Num topics: {n}")
        logger.info(f"The lower bound on the log likelihood of the entire corpus: {ll}")
        logger.info(f"The upper bound on perplexity: {lp}")
        results_log_likelihood.append(ll)
        results_log_perplexity.append(lp)
        save_ml_model(spark=spark, model=cv_model, storage_name=TOPIC_CLUSTERING_STORAGE_NAME,
                      container_name=TOPIC_CLUSTERING_CONTAINER_NAME, output_folder=TOPIC_CLUSTERING_OUTPUT_LDA,
                      output_suffix=n)

    data = {
        "num_topics": list_num_topics,
        "log_likelihood": results_log_likelihood,
        "log_perplexity": results_log_perplexity,
    }
    result_p = pd.DataFrame(data)
    key = spark.conf.get(f"spark.hadoop.fs.azure.account.key.{TOPIC_CLUSTERING_STORAGE_NAME}.blob.core.windows.net")
    save_results_lda(result_p, key=key, list_num_topics=list_num_topics)

    logger.info("Process finished!")
    return result_p


def save_ml_model(spark, model, storage_name: str, container_name: str, output_folder, output_suffix):
    """Saves a spark model to a blob storage"""
    key = spark.conf.get(f"spark.hadoop.fs.azure.account.key.{storage_name}.blob.core.windows.net")
    create_if_not_exists_container(storage_name, container_name=container_name, key=key, logger=logger)
    output_path = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/{output_folder}_{output_suffix}/"
    model.write().overwrite().save(output_path)
    logger.info(f"Model saved in: {output_path}")


def save_results_lda(df_p: pd.DataFrame, key: str, list_num_topics: List[int]):
    """Saves a csv file to blob storage with the information of the results of all topics"""
    output_file = TOPIC_CLUSTERING_OUTPUT_LDA_RESULT_PREFIX + "_".join([str(n) for n in list_num_topics]) + ".csv"
    logger.info(f"Saving local data into {output_file}")
    df_p.to_csv(output_file, header=True, index=False, sep=";", encoding="utf-8")

    logger.info(f"Uploading data...")
    output_url = get_account_url(TOPIC_CLUSTERING_STORAGE_NAME)
    output_service = BlobServiceClient(account_url=output_url, credential=key)
    output_container = output_service.get_container_client(TOPIC_CLUSTERING_CONTAINER_NAME)
    upload_blob_client = output_container.get_blob_client(output_file)
    with open(output_file, "rb") as data:
        upload_blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
    logger.info("Upload completed!")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        list_num_topics = []
        for n in range(1, len(sys.argv)):
            list_num_topics.append(int(sys.argv[n]))
    else:
        list_num_topics = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 30, 50, 100]
    logger.info(f"Executing with the following list of number of topics: {list_num_topics}")
    spark_session = create_spark_session("energy_clustering")
    run_energy_clustering(spark_session, list_num_topics)
