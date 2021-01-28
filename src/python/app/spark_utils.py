# -*- coding: utf-8 -*-
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def flatten_df(df) -> DataFrame:
    df = df.select(flatten_schema(df.schema))
    return df


def flatten_schema(schema: DataFrame.schema, prefix=None):
    """Flattens StructFields into simple fields"""
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(sf.col(name).alias(name))
    return fields