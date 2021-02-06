# -*- coding: utf-8 -*-
# INPUT DATA
INPUT_STORAGE_NAME = "challengebasf"
INPUT_STORAGE_KEY = ""
INPUT_STORAGE_URL = f"https://{INPUT_STORAGE_NAME}.blob.core.windows.net/"
INPUT_CONTAINER_NAME = "rawdata"

# SANITIZED DATA
SANITIZED_STORAGE_NAME = INPUT_STORAGE_NAME
SANITIZED_STORAGE_KEY = INPUT_STORAGE_KEY
SANITIZED_CONTAINER_NAME = "sanitizeddata"

# PARQUET DATA
PARQUET_STORAGE_NAME = INPUT_STORAGE_NAME
PARQUET_STORAGE_KEY = INPUT_STORAGE_KEY
PARQUET_CONTAINER_NAME = "parquetdata"
PARQUET_OUTPUT_FOLDER = "output_data"

# FILTERED DATA
FILTERED_STORAGE_NAME = INPUT_STORAGE_NAME
FILTERED_STORAGE_KEY = INPUT_STORAGE_KEY
FILTERED_CONTAINER_NAME = "filtereddata"
FILTERED_OUTPUT_FOLDER = "output_data"

# PROCESSED TEXT FROM FILTERED DATA
PROCESSED_TEXT_STORAGE_NAME = INPUT_STORAGE_NAME
PROCESSED_TEXT_STORAGE_KEY = INPUT_STORAGE_KEY
PROCESSED_TEXT_CONTAINER_NAME = "processedtextdata"
PROCESSED_TEXT_OUTPUT_FOLDER = "output_data"

# FREQUENT_WORDS
FREQUENT_WORDS_STORAGE_NAME = INPUT_STORAGE_NAME
FREQUENT_WORDS_STORAGE_KEY = INPUT_STORAGE_KEY
FREQUENT_WORDS_CONTAINER_NAME = "frequentwords"
FREQUENT_WORDS_OUTPUT_FILE_NAME = "frequent_words.csv"  # Has to end with .csv

# PROCESS DATA FOR CLASSIFIER AND CLUSTEING
PROCESSED_STORAGE_NAME = INPUT_STORAGE_NAME
PROCESSED_STORAGE_KEY = INPUT_STORAGE_KEY
PROCESSED_CONTAINER_NAME = "proceseddata"
PROCESSED_OUTPUT_FOLDER = "output_data"

# POSITIVE DATA FOR ENERGY CLASSIFIER
ENERGY_CLASSIFIER_STORAGE_NAME = "hiringchallengespatents"
ENERGY_CLASSIFIER_STORAGE_KEY = ""
ENERGY_CLASSIFIER_CONTAINER_NAME = "training-data"
ENERGY_CLASSIFIER_FILE_NAME = "energy.consumption.patent.filenames"


DEFAULT_STORAGE_FORMAT = "parquet"
