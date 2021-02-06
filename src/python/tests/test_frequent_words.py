# -*- coding: utf-8 -*-
import os
import pandas as pd
from pyspark.sql import functions as sf

from ..app.frequent_words import process
from ..app.filter_english_patents import OUTPUT_COL_ENGLISH_TEXT


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

INPUT_FOLDER = "data_frequent_words/"
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), INPUT_FOLDER)


def test_process(spark):
    text1 = "the patent is a document"
    text2 = "this is a patent classifier"
    text3 = "text has to be cleaned in this step"
    col = f"{OUTPUT_COL_ENGLISH_TEXT}_stopwords"
    data = {
        "_file": [1, 2, 3],
        col: [text1, text2, text3]
    }
    df = spark.createDataFrame(pd.DataFrame(data))
    df = df.withColumn(col, sf.split(sf.col(col), " "))
    result_p = process(df)
    expected = {  # Stopwords removal is done in a previous step. This process only counts words
        "the": 1,
        "patent": 2,
        "is": 2,
        "a": 2,
        "document": 1,
        "this": 2,
        "classifier": 1,
        "text": 1,
        "has": 1,
        "to": 1,
        "be": 1,
        "cleaned": 1,
        "in": 1,
        "step": 1
    }
    words = []
    nums = []
    for word, num in expected.items():
        words.append(word)
        nums.append(num)
    expected_output_p = pd.DataFrame({"word": words, "count": nums})
    check = pd.merge(result_p, expected_output_p, on=result_p.columns.tolist(), how="outer", indicator=True)
    errors = check[check["_merge"] != "both"]
    assert errors.empty, f"There are the following errors:\n{errors.to_string()}"
