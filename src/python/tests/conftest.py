# -*- coding: utf-8 -*-
import findspark
findspark.init()
import pytest
from ..app.utils import create_spark_session


@pytest.fixture(scope='session')
def spark():
    config = {"spark.jars.packages": "com.databricks:spark-xml_2.11:0.11.0"}
    spark = create_spark_session("tests", local=True, config=config)
    yield spark
    spark.stop()
