# -*- coding: utf-8 -*-
import findspark
findspark.init()
import os
import pytest
from utils import create_spark_session, zip_app, ZIP_FILE_NAME, get_app_path, get_resources_path


@pytest.fixture(scope='session')
def spark():
    jar_xml = os.path.join(get_resources_path(), "spark-xml_2.11-0.11.0.jar")
    # jar_nlp = os.path.join(get_resources_path(), "spark-nlp_2.11-2.7.2.jar")
    config = {
        "spark.jars": f"{jar_xml}",
        "spark.jars.packages": "com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.2",
    }
    spark = create_spark_session("tests", local=True, config=config)
    zip_app()
    spark.sparkContext.addPyFile(os.path.join(get_app_path(), ZIP_FILE_NAME))
    yield spark
    spark.stop()
