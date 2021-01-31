# -*- coding: utf-8 -*-
"""
Script to remove all non english patents. All documents have at least a title and abstract written in English
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql import functions as sf
from constants import PARQUET_CONTAINER_NAME, PARQUET_STORAGE_NAME, PARQUET_OUTPUT_FOLDER, \
    FILTERED_STORAGE_NAME, FILTERED_STORAGE_KEY, FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER
from launcher import logger
from utils import create_spark_session
from spark_utils import flatten_df, udf_get_text_from_col
from azure_utils import create_if_not_exists_container


LOGGER_CHILD_NAME = "FILTER_ENGLISH_PATENTS"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 50  # TODO parametrize

OUTPUT_COL_ENGLISH_ABSTRACT = "english_abstract"
OUTPUT_COL_ENGLISH_TITLE = "english_title"
OUTPUT_COL_ENGLISH_DESCRIPTION = "english_description"
OUTPUT_COL_ENGLISH_TEXT = "english_text"


def run_filter_english_patents(spark: SparkSession):
    logger.info("Starting execution")
    create_if_not_exists_container(storage_name=FILTERED_STORAGE_NAME, key=FILTERED_STORAGE_KEY,
                                   container_name=FILTERED_CONTAINER_NAME, logger=logger)
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
    df = process_descriptions(df)
    df = create_text_column(df)
    df = flatten_df(df)  # Flatten data to improve performance in analytic use
    return df


def filter_abstracts(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english abstract and creates a column with only the english version
    """
    log_language_distribution(df=df, field_name="abstract")

    col_abstract = sf.col("abstract")
    df = df.withColumn("num_english_abstract", get_num_english_element(col_abstract))
    cond_abstract_language = (~sf.col("num_english_abstract").isNull()) & (sf.col("num_english_abstract") >= 0)
    df = df.filter(cond_abstract_language)
    df = df.withColumn(OUTPUT_COL_ENGLISH_ABSTRACT, col_abstract.getItem(sf.col("num_english_abstract")))
    cond_abstract_text_not_null = sf.size(sf.col(f"{OUTPUT_COL_ENGLISH_ABSTRACT}.p")) > 0
    df = df.filter(cond_abstract_text_not_null)
    return df


def filter_titles(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english title and creates a column with only the english version
    """
    log_language_distribution(df=df, field_name="bibliographic-data.invention-title")

    col_titles = sf.col("bibliographic-data.invention-title")
    df = df.withColumn("num_english_title", get_num_english_element(col_titles))
    cond_title_language = (~sf.col("num_english_title").isNull()) & (sf.col("num_english_title") >= 0)
    df = df.filter(cond_title_language)
    df = df.withColumn(OUTPUT_COL_ENGLISH_TITLE, col_titles.getItem(sf.col("num_english_title")))
    return df


def process_descriptions(df: DataFrame) -> DataFrame:
    """
    Creates a column with only english descriptions.
    In case of non english descriptions several versions of the description are provided.
      <description format="machine_translation" lang="en" id="desc_en">...</description>
      <description format="original" lang="fr" id="desc_fr">...</description>
    """
    log_language_distribution(df=df, field_name="description")

    col_titles = sf.col("description")
    df = df.withColumn("num_english_description", get_num_english_element(col_titles))
    # We are not filtering patents without english description.
    # We choose english version if possible and if not the original one
    col = sf.when(sf.col("num_english_description") >= 0,
                  col_titles.getItem(sf.col("num_english_description"))).otherwise(col_titles.getItem(0))
    df = df.withColumn(OUTPUT_COL_ENGLISH_DESCRIPTION, col)
    return df


def create_text_column(df: DataFrame) -> DataFrame:
    """Creates a column with all the english text of the patent"""
    col_text_title = sf.col(f"{OUTPUT_COL_ENGLISH_TITLE}._VALUE")  # Text
    col_text_abstract = sf.col(f"{OUTPUT_COL_ENGLISH_ABSTRACT}.p")  # Array
    col_text_description = sf.col(f"{OUTPUT_COL_ENGLISH_DESCRIPTION}.p")  # Array
    df = df.withColumn("title_text", udf_get_text_from_col(col_text_title))
    df = df.withColumn("abstract_text", udf_get_text_from_col(col_text_abstract))
    df = df.withColumn("description_text", udf_get_text_from_col(col_text_description))
    df = df.withColumn(OUTPUT_COL_ENGLISH_TEXT,
                       sf.concat_ws(" ", sf.col("title_text"), sf.col("abstract_text"), sf.col("description_text")))
    df = df.withColumn(OUTPUT_COL_ENGLISH_TEXT,
                       sf.lower(sf.regexp_replace(OUTPUT_COL_ENGLISH_TEXT, "[^a-zA-Z\\s]", "")))
    return df


def log_language_distribution(df: DataFrame, field_name: str):
    assert isinstance(df.select(field_name).schema.fields[0].dataType, ArrayType)
    languages = df.select("_file", sf.explode_outer(field_name).alias("target_field"))
    languages_p = languages.groupby("target_field._lang").agg(sf.count("_file").alias("num")).toPandas()
    logger.info(f"Distribution of languages in {field_name}:\n{languages_p.to_string()}")
    # TODO add a language detector for titles without _lang info


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
