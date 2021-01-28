# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


def create_spark_session(name: str, local=False, config=None) -> SparkSession:
    """Init a spark session"""
    spark = SparkSession.builder

    if local:
        spark = spark.master("local[*]")
    spark = spark.appName(name)
    if config:
        for key, value in config.items():
            spark = spark.config(key, value)
    spark = spark.getOrCreate()
    return spark
