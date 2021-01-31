# -*- coding: utf-8 -*-
import pytest
import pandas as pd
import pyspark.sql.functions as sf
from ..app.spark_utils import flatten_df, normalize_column_names, udf_get_text_from_col


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
    expected_output = [col for col in df.columns if col not in ["struct"]] + ["struct_name", "struct_age"]
    assert result.columns == expected_output
    assert df.count() == result.count()


def test_flatten_schema_nested(spark):
    """Check simple struct columns are flattened"""
    df = spark.createDataFrame(df_p)
    df = df.withColumn("struct", sf.struct(sf.lit("a").alias("name"), sf.lit("b").alias("age")))
    df = df.withColumn("struct_2", sf.struct(sf.col("struct"), sf.lit("b").alias("other")))
    result = flatten_df(df)
    struct_cols = ["struct_name", "struct_age"] + ["struct_2_struct_name", "struct_2_struct_age", "struct_2_other"]
    expected_output = [col for col in df.columns if col not in struct_cols + ["struct", "struct_2"]] + struct_cols
    assert result.columns == expected_output
    assert df.count() == result.count()


@pytest.mark.parametrize("input_names,expected_names", [
    (["name", "age", "_value_"], ["name", "age", "_value_"]),
    (["nA Me", "age ", " _value_ "], ["name", "age", "_value_"]),
    (["nA Me", "age ", " _value_.other"], ["name", "age", "_value__other"]),
])
def test_normalize_column_names(spark, input_names, expected_names):
    df = spark.createDataFrame(pd.DataFrame({name: [1, 2, 3] for name in input_names}))
    result = normalize_column_names(df)
    assert result.columns == expected_names


@pytest.mark.parametrize("input_values", [
    (["name", "age", "_value_"]),
    ([1, 2, 3]),
    ([True, False, True]),
])
def test_get_text_from_col(spark, input_values):
    """Check transformation in simple columns"""
    df = spark.createDataFrame(pd.DataFrame({"column": input_values}))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("column")))
    result_p = result.select("result").toPandas()
    expected_p = pd.DataFrame({"result": [str(value) for value in input_values]})
    pd.testing.assert_frame_equal(result_p, expected_p)


@pytest.mark.parametrize("input_values", [
    (["name", "age", "_value_"]),
    ([1, 2, 3]),
    ([True, False, True]),
])
def test_get_text_from_col_array(spark, input_values):
    df = spark.createDataFrame(pd.DataFrame({"column": ["test"]}))
    df = df.withColumn("array", sf.array(*[sf.lit(value) for value in input_values]))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("array")))
    result_p = result.select("result").toPandas()
    assert result_p.iloc[[0, 0]].values[0] == " ".join([str(value) for value in input_values])


@pytest.mark.parametrize("input_values", [
    (["name", "age", "_value_"]),
    ([1, 2, 3]),
    ([True, False, True]),
])
def test_get_text_from_col_map(spark, input_values):
    df = spark.createDataFrame(pd.DataFrame({"column": ["test"]}))
    df = df.withColumn("map", sf.create_map(
        sf.lit("a"), sf.lit(input_values[0]),
        sf.lit("b"), sf.lit(input_values[1]),
        sf.lit("c"), sf.lit(input_values[2]),
    ))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("map")))
    result_p = result.select("result").toPandas()
    assert result_p.iloc[[0, 0]].values[0] == " ".join([str(value) for value in input_values])


@pytest.mark.parametrize("input_values", [
    (["name", "age", "_value_"]),
    ([1, 2, 3]),
    ([True, False, True]),
])
def test_get_text_from_col_struct(spark, input_values):
    df = spark.createDataFrame(pd.DataFrame({"column": ["test"]}))
    df = df.withColumn("struct", sf.struct(
        sf.lit(input_values[0]).alias("a"),
        sf.lit(input_values[1]).alias("b"),
        sf.lit(input_values[2]).alias("c"),
    ))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("struct")))
    result_p = result.select("result").toPandas()
    assert result_p.iloc[[0, 0]].values[0] == " ".join([str(value) for value in input_values])
    df = df.withColumn("struct", sf.struct(
        sf.lit(input_values[0]).alias("a"),
        sf.lit(input_values[1]).alias("_VALUE"),
        sf.lit(input_values[2]).alias("_c"),
    ))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("struct")))
    result_p = result.select("result").toPandas()
    assert result_p.iloc[[0, 0]].values[0] == " ".join([str(value) for value in input_values[:-1]])


@pytest.mark.parametrize("input_values", [
    (["name", "age", "_value_"]),
    ([1, 2, 3]),
    ([True, False, True]),
])
def test_get_text_from_col_array_struct(spark, input_values):
    df = spark.createDataFrame(pd.DataFrame({"column": ["test"]}))
    struct = sf.struct(
        sf.lit(input_values[0]).alias("a"),
        sf.lit(input_values[1]).alias("b"),
        sf.lit(input_values[2]).alias("c"),
    )
    df = df.withColumn("array_struct", sf.array(struct, struct))
    result = df.withColumn("result", udf_get_text_from_col(sf.col("array_struct")))
    result_p = result.select("result").toPandas()
    assert result_p.iloc[[0, 0]].values[0] == " ".join([str(value) for value in input_values] * 2)
