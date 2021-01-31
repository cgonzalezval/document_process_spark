# -*- coding: utf-8 -*-
import pandas as pd
import pytest
from pyspark.sql import functions as sf

from ..app.filter_english_patents import get_num_english_element


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
