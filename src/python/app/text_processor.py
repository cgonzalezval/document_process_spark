# -*- coding: utf-8 -*-
"""
Script to process the text in the documents and store the results so it is reusable.
Selects only _file and text columns. For each text column generates two additional columns after StopWords and Lemma
"""
from pyspark.sql import DataFrame, SparkSession
from sparknlp.base import *
from sparknlp.annotator import *

from constants import FILTERED_STORAGE_NAME, FILTERED_CONTAINER_NAME, FILTERED_OUTPUT_FOLDER, \
    PROCESSED_TEXT_STORAGE_NAME, PROCESSED_TEXT_CONTAINER_NAME, PROCESSED_TEXT_OUTPUT_FOLDER
from filter_english_patents import OUTPUT_COL_ENGLISH_ABSTRACT_TEXT, OUTPUT_COL_ENGLISH_TITLE_TEXT,\
    OUTPUT_COL_ENGLISH_CLAIMS_TEXT, OUTPUT_COL_ENGLISH_TEXT
from launcher import logger
from utils import create_spark_session
from spark_utils import save, read


LOGGER_CHILD_NAME = "TEXT_PROCESSOR"
logger = logger.getChild(LOGGER_CHILD_NAME)
NUM_OUTPUT_FILES = 200  # TODO parametrize


def run_text_processor(spark: SparkSession):
    logger.info("Starting execution")
    df = read(spark=spark, storage_name=FILTERED_STORAGE_NAME, containter_name=FILTERED_CONTAINER_NAME,
              output_folder=FILTERED_OUTPUT_FOLDER, logger=logger)
    result = process(df)
    save(spark=spark, df=result, num_files=NUM_OUTPUT_FILES, containter_name=PROCESSED_TEXT_CONTAINER_NAME,
         storage_name=PROCESSED_TEXT_STORAGE_NAME, output_folder=PROCESSED_TEXT_OUTPUT_FOLDER, logger=logger)
    logger.info("Process finished!")
    return result


def process(df: DataFrame) -> DataFrame:
    """
    Process text columns and generates two columns per input column with the result after StopWords and Lemmatization
    """
    cols = [OUTPUT_COL_ENGLISH_TEXT, OUTPUT_COL_ENGLISH_ABSTRACT_TEXT, OUTPUT_COL_ENGLISH_TITLE_TEXT,
            OUTPUT_COL_ENGLISH_CLAIMS_TEXT]
    df = df.select("_file", *cols)
    # Initializad only once because it downloads data each time
    lemma = LemmatizerModel.pretrained(name="lemma_antbnc", lang="en").setInputCols(["stopwords"]).setOutputCol("lemma")
    for col in cols:
        logger.info(f"Processing column: {col}")
        df = process_col(df=df, input_col=col, lemma=lemma)
    return df


def process_col(df, input_col, lemma):
    """Creates a column with the result of the StopWords and another after lemmatization"""
    document_assembler = DocumentAssembler().setInputCol(input_col).setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    # clean tokens and lowercase
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalized")
    stopwords_cleaner = StopWordsCleaner().setInputCols("normalized").setOutputCol("stopwords")\
        .setCaseSensitive(False)
    # TODO move to offline
    finisher = Finisher().setInputCols(["lemma"]).setOutputCols([f"{input_col}_features"]).setOutputAsArray(True)\
        .setCleanAnnotations(False)
    finisher2 = Finisher().setInputCols(["stopwords"]).setOutputCols([f"{input_col}_stopwords"]).setOutputAsArray(True)\
        .setCleanAnnotations(False)
    nlp_pipeline = Pipeline(
        stages=[document_assembler,
                tokenizer,
                normalizer,
                stopwords_cleaner,
                lemma,
                finisher,
                finisher2])
    pipeline_model = nlp_pipeline.fit(df)
    result = pipeline_model.transform(df)
    result = result.drop("document", "token", "normalized", "stopwords", "lemma")
    return result


if __name__ == '__main__':
    spark_session = create_spark_session("process_text")
    run_text_processor(spark_session)
