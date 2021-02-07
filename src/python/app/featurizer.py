# -*- coding: utf-8 -*-
"""
Script to generate new features
"""
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.ml.feature import VectorAssembler

from constants import PROCESSED_TEXT_STORAGE_NAME, PROCESSED_TEXT_CONTAINER_NAME, PROCESSED_TEXT_OUTPUT_FOLDER,\
    FILTERED_STORAGE_NAME, FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER, FEATURES_CONTAINER_NAME,\
    FEATURES_STORAGE_NAME, FEATURES_OUTPUT_FOLDER
from utils import create_spark_session
from launcher import logger
from spark_utils import read, save

LOGGER_CHILD_NAME = "FEATURES"
NUM_OUTPUT_FILES = 200  # TODO parametrize

# https://www.wipo.int/classifications/ipc/en/ITsupport/Version20150101/index.html
SECTIONS_IPCR = ["A", "B", "C", "D", "E", "F", "G"]
SECTIONS_CLASS_IPCR = [
    "A01", "A21", "A22", "A23", "A24", "A41", "A42", "A43", "A44", "A45", "A46", "A47", "A61", "A62", "A63", "A99",
    "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09",
    "B21", "B22", "B23", "B24", "B25", "B26", "B27", "B28", "B29", "B30", "B31", "B32",
    "B41", "B42", "B43", "B44", "B60", "B61", "B62", "B63", "B64", "B65", "B66", "B67", "B68", "B81", "B82", "B99",
    "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14",
    "C21", "C22", "C23", "C25", "C30", "C40", "C99",
    "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D21", "D99",
    "E01", "E02", "E03", "E04", "E05", "E06", "E21", "E99",
    "F01", "F02", "F03", "F04", "F15", "F16", "F17", "F21",
    "F22", "F23", "F24", "F25", "F26", "F27", "F28", "F41", "F42", "F99",
    "G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09", "G10", "G11", "G12", "G21", "G99",
    "H01", "H02", "H03", "H04", "H05", "H99",
]

FEATURE_COLS_SECTION = [f"section_{section}" for section in SECTIONS_IPCR]
FEATURE_COLS_SECTION_CLASS = [f"section_class_{section_class}" for section_class in SECTIONS_CLASS_IPCR]

NON_TEXT_FEATURE_COLS = FEATURE_COLS_SECTION + FEATURE_COLS_SECTION_CLASS
TEXT_FEATURE_COLS = ["flag_energy_title", "flag_energy_abstract", "flag_energy_claims"]


def run_featurize_patents(spark: SparkSession):
    logger.info("Starting execution")
    full = read(spark=spark, storage_name=FILTERED_STORAGE_NAME, containter_name=FILTERED_CONTAINER_NAME,
                output_folder=FILTERED_OUTPUT_FOLDER, logger=logger)
    full = process_full(full)

    text = read(spark=spark, storage_name=PROCESSED_TEXT_STORAGE_NAME, containter_name=PROCESSED_TEXT_CONTAINER_NAME,
                output_folder=PROCESSED_TEXT_OUTPUT_FOLDER, logger=logger)
    text = process_text(text)
    total = full.join(text, ["_file"], "inner")

    assembler = VectorAssembler(
        inputCols=NON_TEXT_FEATURE_COLS + TEXT_FEATURE_COLS + ["english_text_features"],
        outputCol="features")
    result = assembler.transform(total)

    save(spark=spark, df=result, num_files=NUM_OUTPUT_FILES, containter_name=FEATURES_CONTAINER_NAME,
         storage_name=FEATURES_STORAGE_NAME, output_folder=FEATURES_OUTPUT_FOLDER, logger=logger)
    logger.info("Process finished!")
    return result


def process_text(df: DataFrame) -> DataFrame:
    """Process features extracted from text fields"""
    df = df.withColumn("flag_energy_title", sf.array_contains("title_text_features", "energy").astype(IntegerType()))
    df = df.withColumn("flag_energy_abstract",
                       sf.array_contains("abstract_text_features", "energy").astype(IntegerType()))
    df = df.withColumn("flag_energy_claims", sf.array_contains("claims_text_features", "energy").astype(IntegerType()))
    # Assembler doesn't support array cols so we have to transform it to vector
    list_to_vector_udf = sf.udf(lambda l: Vectors.dense(l), VectorUDT())
    df = df.withColumn("english_text_features", list_to_vector_udf(sf.col("english_text_features")))
    feature_cols = ["english_text_features", "flag_energy_title", "flag_energy_abstract", "flag_energy_claims"]
    df = df.select("_file", *feature_cols)
    return df


def process_full(df: DataFrame) -> DataFrame:
    """Process features extracted from non-text fields"""
    df = df.select("_file", "bibliographic-data_classifications-ipcr_classification-ipcr")
    df = process_ipcr(df)
    return df


schema = StructType([
    StructField("sections", ArrayType(StringType()), True),
    StructField("sections_class", ArrayType(StringType()), True)
])


@sf.udf(schema)
def extract_ipcr(value):
    """Extract all the sections and section/class of a patent and returns all its possible values for each type"""
    if value is None:
        return []
    # Return type has to be a list. Set gives null result in spark
    sections = list()
    sections_class = list()
    for item in value:
        if item is None:
            pass
        else:
            text = item["text"].strip().upper()
            if text is not None and len(text) > 3:
                sections.append(text[0])
                sections_class.append(item["text"][:3])
    # Remove duplicates
    sections = list(set(sections))
    sections_class = list(set(sections_class))
    return sections, sections_class


def process_ipcr(df: DataFrame) -> DataFrame:
    """
    Generates a flag column for each section and combination of section/class of the patents indicating if the patent
    is part of this categorization
    """
    col = "bibliographic-data_classifications-ipcr_classification-ipcr"
    df = df.withColumn("ipcr_values", extract_ipcr(sf.col(col)))
    df = df.withColumn("ipcr_sections", sf.col("ipcr_values.sections"))
    df = df.withColumn("ipcr_sections_class", sf.col("ipcr_values.sections_class"))
    for section in SECTIONS_IPCR:
        df = df.withColumn(f"section_{section}",
                           sf.array_contains(sf.col("ipcr_sections"), section).astype(IntegerType()))

    for section_class in SECTIONS_CLASS_IPCR:
        df = df.withColumn(f"section_class_{section_class}",
                           sf.array_contains(sf.col("ipcr_sections_class"), section_class).astype(IntegerType()))
    return df


if __name__ == '__main__':
    spark = create_spark_session("features_data")
    run_featurize_patents(spark)
