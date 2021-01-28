# -*- coding: utf-8 -*-
import pandas as pd
import pyspark.sql.functions as sf
from ..app.spark_utils import flatten_df


data = {
    "field1": [1, 2],
    "field2": ["A", "B"],
    "field3": [True, False],
}
df_p = pd.DataFrame(data)


def test_flatten_schema_no_changes(spark):
    """Check non-struct columns are not affected"""
    df = spark.createDataFrame(df_p)
    df = df.withColumn("array", sf.array(sf.lit("a"), sf.lit("-")))
    df = df.withColumn("map", sf.create_map(sf.lit("b"), sf.lit("_")))
    result = flatten_df(df)
    assert df.columns == result.columns
    assert df.count() == result.count()


def test_flatten_schema(spark):
    """Check simple struct columns are flattened"""
    df = spark.createDataFrame(df_p)
    df = df.withColumn("struct", sf.struct(sf.lit("a").alias("name"), sf.lit("b").alias("age")))
    result = flatten_df(df)
    expected_output = [col for col in df.columns if col not in ["struct"]] + ["struct.name", "struct.age"]
    assert result.columns == expected_output
    assert df.count() == result.count()


def test_flatten_schema_nested(spark):
    """Check simple struct columns are flattened"""
    df = spark.createDataFrame(df_p)
    df = df.withColumn("struct", sf.struct(sf.lit("a").alias("name"), sf.lit("b").alias("age")))
    df = df.withColumn("struct_2", sf.struct(sf.col("struct"), sf.lit("b").alias("other")))
    result = flatten_df(df)
    struct_cols = ["struct.name", "struct.age"] + ["struct_2.struct.name", "struct_2.struct.age", "struct_2.other"]
    expected_output = [col for col in df.columns if col not in struct_cols + ["struct", "struct_2"]] + struct_cols
    assert result.columns == expected_output
    assert df.count() == result.count()
