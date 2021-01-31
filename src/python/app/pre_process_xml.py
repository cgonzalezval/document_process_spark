# -*- coding: utf-8 -*-
"""
Script to pre-process xml info.
Steps:
    - Download tarfile
    - Extract xml files
    - Clean xml files
        - HTML tags in text fields
    - Create one xml file containing all xmls in tar
    - Upload file
"""
import shutil
import os
import tarfile
from typing import Tuple
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from constants import INPUT_STORAGE_NAME, INPUT_STORAGE_KEY, INPUT_CONTAINER_NAME, \
    SANITIZED_STORAGE_NAME, SANITIZED_STORAGE_KEY, SANITIZED_CONTAINER_NAME
from launcher import logger
from utils import create_spark_session, sanitize_xml
from azure_utils import get_account_url

output_tmp_folder_blob = "./downloaded_blobs"
output_tmp_folder_xmls = "./downloaded_xmls"

LOGGER_CHILD_NAME = "SANITIZE_DATA"
logger = logger.getChild(LOGGER_CHILD_NAME)


def run_sanitize_data():
    logger.info("Starting execution")
    input_url = get_account_url(INPUT_STORAGE_NAME)
    service = BlobServiceClient(account_url=input_url, credential=INPUT_STORAGE_KEY)
    container = service.get_container_client(INPUT_CONTAINER_NAME)
    output_url = get_account_url(SANITIZED_STORAGE_NAME)
    output_service = BlobServiceClient(account_url=output_url, credential=SANITIZED_STORAGE_KEY)
    output_container = output_service.get_container_client(SANITIZED_CONTAINER_NAME)
    try:
        output_container.create_container()
        logger.info(f"Creating container: {SANITIZED_CONTAINER_NAME}")
    except ResourceExistsError:
        logger.warning("Output container already exists")

    blobs = list(container.list_blobs())
    logger.info(f"There are {len(blobs)} blobs to process")
    info_unzip_num_files = []
    for n, blob in enumerate(blobs):
        try:
            blob_name = blob["name"]
            logger.info(f"Processing blob {n + 1} of {len(blobs)}: {blob_name}")
        except KeyError:
            logger.error(f"Omitting blob, it doesn't have a name: {blob}")
            continue
        blob = container.get_blob_client(blob=blob_name)
        init_local_directories()

        # Download
        target_blob_file = os.path.join(output_tmp_folder_blob, blob_name)
        logger.info(f"Downloading {blob_name} into {target_blob_file}")
        download_data(blob=blob, target_file=target_blob_file)

        # Process
        unzip_info = unzip_data(target_file=target_blob_file, output_folder=output_tmp_folder_xmls)
        info_unzip_num_files.append(unzip_info)
        output_xml_file = os.path.splitext(os.path.basename(blob_name))[0] + ".xml"
        process(input_folder=output_tmp_folder_xmls, output_file=output_xml_file)

        # Upload
        upload_blob_client = output_container.get_blob_client(output_xml_file)
        with open(output_xml_file, "rb") as data:
            upload_blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
        logger.info("Upload completed!")
        os.remove(output_xml_file)
        logger.info("Local file deleted!")

    logger.info("Uploaded info:")
    total_num_registers = 0
    for name, num_files in info_unzip_num_files:
        logger.info(f"File {name} -> {num_files} registers")
        total_num_registers += num_files
    logger.info(f"Total registers uploaded: {total_num_registers}")
    logger.info("Process finished!")


def init_local_directories():
    if os.path.exists(output_tmp_folder_xmls):
        shutil.rmtree(output_tmp_folder_xmls)
    if os.path.exists(output_tmp_folder_blob):
        shutil.rmtree(output_tmp_folder_blob)
    os.makedirs(output_tmp_folder_blob)
    os.makedirs(output_tmp_folder_xmls)


def download_data(blob, target_file: str):
    with open(target_file, "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)
    logger.info(f"Download completed!")


def unzip_data(target_file: str, output_folder: str) -> Tuple[str, int]:
    num_files = 0
    if target_file.endswith("tgz"):
        tar = tarfile.open(target_file, "r:gz")
        for member in tar.getmembers():
            if member.isreg():  # skip if the TarInfo is not files
                member.name = os.path.basename(member.name)  # remove the path by reset it
                logger.debug(f"Extracting {member.name} into {output_folder}")
                tar.extract(member, output_folder)
                num_files += 1
            else:
                logger.info(f"Omitting file {member.name}")
    else:
        logger.error(f"Unsupported file extension: {target_file}")
    logger.info(f"Unzip completed! {num_files} files into {target_file}")
    return target_file, num_files


def process(input_folder: str, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        for (root, directories, file_names) in os.walk(input_folder):
            for file in file_names:
                if file.endswith(".xml"):  # To exclude xds and other files
                    with open(os.path.join(root, file), "r", encoding="utf-8") as input_file:
                        data = input_file.read()
                    output = sanitize_xml(data)
                    f.write(output)
    logger.info("Process completed!")


if __name__ == '__main__':
    run_sanitize_data()
    # Spark session only to be launched in a azure databricks cluster. TODO move to an external MV
    spark_session = create_spark_session("pre-process-xmls", local=True)
