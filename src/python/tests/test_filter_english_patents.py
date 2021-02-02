# -*- coding: utf-8 -*-
import os
import pandas as pd
import pytest
from pyspark.sql import functions as sf

from ..app.filter_english_patents import get_num_english_element, process

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

INPUT_FOLDER = "data_filter_english_patents/"
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), INPUT_FOLDER)


@pytest.mark.parametrize("data1,col1_name,data2,col2_name, expected", [
    (["a"], "value", ["en"], "_lang", 0),
    (["a"], "value", ["fr"], "_lang", -1),
    (["a", "b"], "value", ["en", "fr"], "_lang", 0),
    (["a", "b"], "value", ["fr", "en"], "_lang", 1),
    (["a", "b"], "value", ["en", "en"], "_lang", 0),
    (["a", "b"], "value", [None, "en"], "_lang", 1),
    (["a", "b"], "value", [None, "fr"], "_lang", -1),
    (["a", "b"], "value", [None, "en"], "_lang2", -1),
])
def test_get_num_english_element(spark, data1, col1_name, data2, col2_name, expected):
    df = spark.createDataFrame(pd.DataFrame({"col1": data1, "col2": data2, "id": [1] * len(data1)}))
    df = df.withColumn("struct", sf.struct(sf.col("col1").alias(col1_name), sf.col("col2").alias(col2_name)))
    df = df.groupby("id").agg(sf.collect_list("struct").alias("array_struct"))
    df = df.withColumn("result", get_num_english_element(sf.col("array_struct")))
    df_p = df.select("result").toPandas()
    assert df_p.iloc[0, 0] == expected


def test_process(spark):
    df = spark.read.format("com.databricks.spark.xml").option("rowTag", "questel-patent-document").load(INPUT_FOLDER)
    assert df.count() == 7
    result = process(df)
    df_p = df.toPandas()
    result_p = result.toPandas()
    expected_output_p = df_p[df_p["expected_output"] == 1]
    cols = ["_file"]
    check = pd.merge(result_p, expected_output_p, on=cols, how="outer", indicator=True)
    errors = check[check["_merge"] != "both"]
    assert errors.empty, f"There are the following errors:\n{errors.to_string()}"
