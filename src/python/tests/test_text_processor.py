# -*- coding: utf-8 -*-
import pandas as pd

from ..app.text_processor import process_col

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def test_process(spark):
    text = "the patent is a Document or documents"
    data = {
        "_file": [1],
        "text": [text]
    }
    input_df_p = pd.DataFrame(data)
    df = spark.createDataFrame(input_df_p)
    result = process_col(df, input_col="text")

    assert "text_stopwords" in result.columns
    assert "text_features" in result.columns

    result_p = result.toPandas()
    assert result_p["text"].values[0] == text
    assert result_p["text_stopwords"].values[0] == ["patent", "document", "documents"]
    assert result_p["text_features"].values[0] == ["patent", "document", "document"]
