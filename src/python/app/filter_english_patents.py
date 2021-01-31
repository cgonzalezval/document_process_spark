# -*- coding: utf-8 -*-
"""
Script to remove all non english patents. All documents have at least a title and abstract written in English
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql import functions as sf
from constants import PARQUET_CONTAINER_NAME, PARQUET_STORAGE_NAME, PARQUET_OUTPUT_FOLDER, FILTERED_STORAGE_NAME, \
    FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER
from launcher import logger
from utils import create_spark_session
from spark_utils import flatten_df


LOGGER_CHILD_NAME = "FILTER_ENGLISH_PATENTS"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 50  # TODO parametrize


def run_filter_english_patents(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark)
    result = process(df)
    save(df=result, num_files=NUM_OUTPUT_FILES)
    logger.info("Process finished!")
    return result


def read(spark: SparkSession) -> DataFrame:
    input_container = f"wasbs://{PARQUET_CONTAINER_NAME}@{PARQUET_STORAGE_NAME}.blob.core.windows.net"
    input_path = f"{input_container}/{PARQUET_OUTPUT_FOLDER}/"
    logger.info(f"Reading from: {input_path}")
    df = spark.read.parquet(input_path)
    return df


def process(df: DataFrame) -> DataFrame:
    """Filter all patents without at least one title and abstract in English and flattens the schema of the result"""
    df = filter_abstracts(df)
    df = filter_titles(df)
    df = flatten_df(df)  # Flatten data to improve performance in analytic use
    return df


def filter_abstracts(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english abstract and overwrites the column with only the english version
    """
    assert isinstance(df.select("abstract").schema.fields[0].dataType, ArrayType)
    abstracts = df.select("_file", sf.explode_outer("abstract").alias("abstract"))
    abstracts_p = abstracts.groupby("abstract._lang").agg(sf.count("_file").alias("num")).toPandas()
    logger.info(f"Distribution of languages in abstracts:\n{abstracts_p.to_string()}")
    # TODO add a language detector for abstracts without _lang info

    col_abstract = sf.col("abstract")
    df = df.withColumn("num_english_abstract", get_num_english_element(col_abstract))
    cond_abstract_language = (~sf.col("num_english_abstract").isNull()) & (sf.col("num_english_abstract") >= 0)
    df = df.filter(cond_abstract_language)
    df = df.withColumn("abstract", col_abstract.getItem(sf.col("num_english_abstract")))
    cond_abstract_text_not_null = sf.size(sf.col("abstract.p")) > 0
    df = df.filter(cond_abstract_text_not_null)
    return df


def filter_titles(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english title and overwrites the column with only the english version
    """
    assert isinstance(df.select("bibliographic-data.invention-title").schema.fields[0].dataType, ArrayType)
    titles = df.select("_file", sf.explode_outer("bibliographic-data.invention-title").alias("title"))
    titles_p = titles.groupby("title._lang").agg(sf.count("_file").alias("num")).toPandas()
    logger.info(f"Distribution of languages in titles:\n{titles_p.to_string()}")
    # TODO add a language detector for titles without _lang info

    col_titles = sf.col("bibliographic-data.invention-title")
    df = df.withColumn("num_english_title", get_num_english_element(col_titles))
    cond_title_language = (~sf.col("num_english_title").isNull()) & (sf.col("num_english_title") >= 0)
    df = df.filter(cond_title_language)
    df = df.withColumn("english_title", col_titles.getItem(sf.col("num_english_title")))
    return df


@sf.udf(IntegerType())
def get_num_english_element(list_elements):
    """Gets the position in the array of an english title of the patent. Returns -1 if there is no english title"""
    if list_elements is None:
        return None
    for n, item in enumerate(list_elements):
        try:
            if item["_lang"] is not None and item["_lang"].lower().strip() == "en":
                return n
        except ValueError:  # If the struct doesn't have a _lang field
            pass
    return -1


def save(df: DataFrame, num_files: int):
    output_container_path = f"wasbs://{FILTERED_CONTAINER_NAME}@{FILTERED_STORAGE_NAME}.blob.core.windows.net"
    output_blob_folder = f"{output_container_path}/{FILTERED_OUTPUT_FOLDER}/"
    logger.info(f"Saving data into {output_blob_folder}")
    df.coalesce(num_files).write.mode("overwrite").parquet(output_blob_folder)
    logger.info(f"Data saved!")


if __name__ == '__main__':
    spark_session = create_spark_session("filter_english_patents")
    run_filter_english_patents(spark_session)
