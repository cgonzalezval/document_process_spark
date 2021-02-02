# -*- coding: utf-8 -*-
import os
import re
import zipfile
from pyspark.sql import SparkSession

ZIP_FILE_NAME = "app.zip"


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
                # & entity names are corrupted and dropped when parsing the xml
                text_to_sanitize = text_to_sanitize.replace("&", "")
                if "CDATA" not in text_to_sanitize:
                    # ]]> only can be used as end of a CDATA.
                    # In any other case is an error and the xml parser will drop the register
                    text_to_sanitize = re.sub("]]>", "", text_to_sanitize, flags=re.DOTALL)
                output_data += text_to_sanitize + "</p>"
                data = data[p_end + 4:]  # 4 positions for </p>
            else:  # Cases to ignore i.e <pub
                pass
        else:
            pending = False
            output_data += data

    return output_data


def get_app_path() -> str:
    """Returns the path to the app folder"""
    path = os.path.dirname(os.path.realpath(__file__))
    return path


def get_resources_path():
    """Returns the path to the resources folder"""
    app_path = get_app_path()
    path = os.path.abspath(os.path.join(app_path, "../../resources"))
    return path


def zip_app(target_file=ZIP_FILE_NAME):
    """Zip file with all the code to be distributed in the spark cluster"""
    def zipdir(path: str, ziph: zipfile.ZipFile):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".py"):
                    ziph.write(os.path.join(root, file), file)

    target_folder = get_app_path()
    try:
        os.remove(target_file)
    except FileNotFoundError:
        pass
    zipf = zipfile.ZipFile(target_file, "w", zipfile.ZIP_DEFLATED)
    zipdir(target_folder, zipf)
    zipf.close()
