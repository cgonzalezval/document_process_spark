# -*- coding: utf-8 -*-
import re
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


def sanitize_xml(data: str) -> str:
    """Removes all html tags inside <p> tags"""
    output_data = ""
    pending = True
    while pending:
        p_init = data.find("<p")
        if p_init != -1:
            output_data += data[:p_init + 2]
            data = data[p_init + 2:]
            if data[0] in [" ", ">"]:  # To match only with <p id=... or <p>, not <pub>
                position_end_tag = data.find(">") + 1  # 1 position to include >
                output_data += data[:position_end_tag]
                data = data[position_end_tag:]

                p_end = data.find("</p>")
                text_to_sanitize = data[:p_end]
                # Check ig there are nested <p>
                num_sub_p = text_to_sanitize.count("<p ") + text_to_sanitize.count("<p>")
                for n in range(num_sub_p):
                    p_end = data.find("</p>", p_end + 4)
                text_to_sanitize = data[:p_end]
                text_to_sanitize = re.sub("<[^>]*>", "", text_to_sanitize, flags=re.DOTALL)
                output_data += text_to_sanitize + "</p>"
                data = data[p_end + 4:]  # 4 positions for </p>
            else:  # Cases to ignore i.e <pub
                pass
        else:
            pending = False
            output_data += data

    return output_data

# data[p_init + 2]
# data[:p_init + 2]
# data[p_init + 2:]
