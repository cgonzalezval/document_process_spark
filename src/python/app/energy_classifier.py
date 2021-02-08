# -*- coding: utf-8 -*-
"""
Script to predict which patents are about energy consumption
"""
from pyspark.sql import functions as sf
from pyspark.sql import DataFrame, SparkSession
from pyspark.ml import PipelineModel

from constants import ENERGY_CLASSIFIER_STORAGE_NAME, ENERGY_CLASSIFIER_CONTAINER_NAME, \
    ENERGY_CLASSIFIER_OUTPUT_FOLDER, ENERGY_PATENTS_STORAGE_NAME, ENERGY_PATENTS_CONTAINER_NAME, \
    ENERGY_PATENTS_OUTPUT_FOLDER, FEATURES_STORAGE_NAME, FEATURES_CONTAINER_NAME, FEATURES_OUTPUT_FOLDER
from launcher import logger
from utils import create_spark_session
from spark_utils import read, save

LOGGER_CHILD_NAME = "ENERGY_CLASSIFIER"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 200


def run_energy_classifier(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark=spark, storage_name=FEATURES_STORAGE_NAME, containter_name=FEATURES_CONTAINER_NAME,
              output_folder=FEATURES_OUTPUT_FOLDER, logger=logger)
    result = process(df)
    save(spark=spark, df=result, num_files=NUM_OUTPUT_FILES, containter_name=ENERGY_PATENTS_CONTAINER_NAME,
         storage_name=ENERGY_PATENTS_STORAGE_NAME, output_folder=ENERGY_PATENTS_OUTPUT_FOLDER, logger=logger)
    logger.info("Process finished!")
    return result


def process(df: DataFrame) -> DataFrame:
    container_path = f"wasbs://{ENERGY_CLASSIFIER_CONTAINER_NAME}@{ENERGY_CLASSIFIER_STORAGE_NAME}.blob.core.windows.net"
    blob_folder = f"{container_path}/{ENERGY_CLASSIFIER_OUTPUT_FOLDER}/"
    model = PipelineModel.load(blob_folder)
    result = model.transform(df)
    result = result.cache()
    num_pos = result.filter(sf.col("prediction") == 1)
    num_neg = result.filter(sf.col("prediction") == 0)
    if num_pos == 0:  # TODO parametrize. Maybe min percentage?
        logger.warning(f"There are {num_pos} positives")
    else:
        logger.info(f"There are {num_pos} positives")
    if num_neg == 0:  # TODO parametrize. Maybe min percentage?
        logger.warning(f"There are {num_neg} negatives")
    else:
        logger.info(f"There are {num_neg} negatives")
    return result


if __name__ == '__main__':
    spark_session = create_spark_session("energy_classifier")
    run_energy_classifier(spark_session)
