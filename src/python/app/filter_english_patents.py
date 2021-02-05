# -*- coding: utf-8 -*-
"""
Script to remove all non english patents. All documents have at least a title and abstract written in English
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql import functions as sf
from constants import PARQUET_CONTAINER_NAME, PARQUET_STORAGE_NAME, PARQUET_OUTPUT_FOLDER, \
    FILTERED_STORAGE_NAME, FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER
from launcher import logger
from utils import create_spark_session
from spark_utils import flatten_df, udf_get_text_from_col, save, read


LOGGER_CHILD_NAME = "FILTER_ENGLISH_PATENTS"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 200  # TODO parametrize

OUTPUT_COL_ENGLISH_ABSTRACT = "english_abstract"
OUTPUT_COL_ENGLISH_TITLE = "english_title"
OUTPUT_COL_ENGLISH_DESCRIPTION = "english_description"
OUTPUT_COL_ENGLISH_CLAIMS = "english_claims"
OUTPUT_COL_ENGLISH_TITLE_TEXT = "title_text"
OUTPUT_COL_ENGLISH_ABSTRACT_TEXT = "abstract_text"
OUTPUT_COL_ENGLISH_DESCRIPTION_TEXT = "description_text"
OUTPUT_COL_ENGLISH_CLAIMS_TEXT = "claims_text"
OUTPUT_COL_ENGLISH_TEXT = "english_text"


def run_filter_english_patents(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark=spark, storage_name=PARQUET_STORAGE_NAME, containter_name=PARQUET_CONTAINER_NAME,
              output_folder=PARQUET_OUTPUT_FOLDER, logger=logger)
    result = process(df)
    save(spark=spark, df=result, num_files=NUM_OUTPUT_FILES, containter_name=FILTERED_CONTAINER_NAME,
         storage_name=FILTERED_STORAGE_NAME, output_folder=FILTERED_OUTPUT_FOLDER, logger=logger)
    logger.info("Process finished!")
    return result


def process(df: DataFrame) -> DataFrame:
    """Filter all patents without at least one title and abstract in English and flattens the schema of the result"""
    df = filter_abstracts(df)
    df = filter_titles(df)
    df = process_descriptions_claims(df, input_field="description", output_field=OUTPUT_COL_ENGLISH_DESCRIPTION)
    df = process_descriptions_claims(df, input_field="claims", output_field=OUTPUT_COL_ENGLISH_CLAIMS)
    df = create_text_column(df)
    df = flatten_df(df)  # Flatten data to improve performance in analytic use
    return df


def filter_abstracts(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english abstract and creates a column with only the english version
    """
    log_language_distribution(df=df, field_name="abstract")

    target_col = sf.col("abstract")
    # Check it is array and generate otherwise. It is needed for the get_num_english_element
    if not isinstance(df.select(target_col).schema.fields[0].dataType, ArrayType):
        df = df.withColumn("abstract", sf.array(target_col))
    df = df.withColumn("num_english_abstract", get_num_english_element(target_col))
    cond_abstract_language = (~sf.col("num_english_abstract").isNull()) & (sf.col("num_english_abstract") >= 0)
    df = df.filter(cond_abstract_language)
    df = df.withColumn(OUTPUT_COL_ENGLISH_ABSTRACT, target_col.getItem(sf.col("num_english_abstract")))
    cond_abstract_text_not_null = sf.size(sf.col(f"{OUTPUT_COL_ENGLISH_ABSTRACT}.p")) > 0
    df = df.filter(cond_abstract_text_not_null)
    df = df.drop("abstract")
    return df


def filter_titles(df: DataFrame) -> DataFrame:
    """
    Removes all patents without at least one english title and creates a column with only the english version
    """
    log_language_distribution(df=df, field_name="bibliographic-data.invention-title")

    target_col = sf.col("bibliographic-data.invention-title")
    # Check it is array and generate otherwise. It is needed for the get_num_english_element
    if not isinstance(df.select(target_col).schema.fields[0].dataType, ArrayType):
        df = df.withColumn("bibliographic-data.invention-title", sf.array(target_col))
    df = df.withColumn("num_english_title", get_num_english_element(target_col))
    cond_title_language = (~sf.col("num_english_title").isNull()) & (sf.col("num_english_title") >= 0)
    df = df.filter(cond_title_language)
    df = df.withColumn(OUTPUT_COL_ENGLISH_TITLE, target_col.getItem(sf.col("num_english_title")))
    df = df.drop("bibliographic-data.invention-title")
    return df


def process_descriptions_claims(df: DataFrame, input_field: str, output_field: str) -> DataFrame:
    """
    Creates a column with only english descriptions/claims.
    In case of non english descriptions several versions of the description are provided.
      <description format="machine_translation" lang="en" id="desc_en">...</description>
      <description format="original" lang="fr" id="desc_fr">...</description>
    """
    log_language_distribution(df=df, field_name=input_field)

    target_col = sf.col(input_field)
    # Check it is array and generate otherwise. It is needed for the get_num_english_element
    if not isinstance(df.select(target_col).schema.fields[0].dataType, ArrayType):
        df = df.withColumn(input_field, sf.array(target_col))

    col_name_num_english = f"num_english_{input_field}"
    df = df.withColumn(col_name_num_english, get_num_english_element(target_col))
    # We are not filtering patents without english description/claim.
    # We choose english version if possible and if not the original one
    col = sf.when(sf.col(col_name_num_english) >= 0,
                  target_col.getItem(sf.col(col_name_num_english))).otherwise(target_col.getItem(0))
    df = df.withColumn(output_field, col)
    df = df.drop(input_field)
    return df


def create_text_column(df: DataFrame) -> DataFrame:
    """Creates a column with all the english text of the patent"""
    col_text_title = sf.col(f"{OUTPUT_COL_ENGLISH_TITLE}._VALUE")  # Text
    col_text_abstract = sf.col(f"{OUTPUT_COL_ENGLISH_ABSTRACT}.p")  # Array
    col_text_description = sf.col(f"{OUTPUT_COL_ENGLISH_DESCRIPTION}.p")  # Array
    col_text_claims = sf.col(f"{OUTPUT_COL_ENGLISH_CLAIMS}.claim")  # Array
    df = df.withColumn(OUTPUT_COL_ENGLISH_TITLE_TEXT, udf_get_text_from_col(col_text_title))
    df = df.withColumn(OUTPUT_COL_ENGLISH_ABSTRACT_TEXT, udf_get_text_from_col(col_text_abstract))
    df = df.withColumn(OUTPUT_COL_ENGLISH_DESCRIPTION_TEXT, udf_get_text_from_col(col_text_description))
    df = df.withColumn(OUTPUT_COL_ENGLISH_CLAIMS_TEXT, udf_get_text_from_col(col_text_claims))
    df = df.withColumn(OUTPUT_COL_ENGLISH_TEXT,
                       sf.concat_ws(" ", sf.col("title_text"), sf.col("abstract_text"), sf.col("description_text")))
    df = df.withColumn(OUTPUT_COL_ENGLISH_TEXT,
                       sf.lower(sf.regexp_replace(OUTPUT_COL_ENGLISH_TEXT, "[^a-zA-Z\\s]", " ")))
    return df


def log_language_distribution(df: DataFrame, field_name: str):
    df = df.cache()
    if isinstance(df.select(field_name).schema.fields[0].dataType, ArrayType):
        languages = df.select("_file", sf.explode_outer(field_name).alias("target_field"))
    else:
        languages = df.select("_file", sf.col(field_name).alias("target_field"))
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
            if item is None:
                pass
            elif item["_lang"] is not None and item["_lang"].lower().strip() == "en":
                return n
        except ValueError:  # If the struct doesn't have a _lang field
            pass
    return -1


if __name__ == '__main__':
    spark_session = create_spark_session("filter_english_patents")
    run_filter_english_patents(spark_session)
