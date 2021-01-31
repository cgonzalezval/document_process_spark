# -*- coding: utf-8 -*-
from logging import Logger
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


def create_if_not_exists_container(storage_name: str, key: str, container_name: str, logger: Logger):
    input_url = get_account_url(storage_name)
    service = BlobServiceClient(account_url=input_url, credential=key)
    container = service.get_container_client(container_name)
    try:
        container.create_container()
        logger.info(f"Creating container: {container_name}")
    except ResourceExistsError:
        logger.warning("Output container already exists")


def get_account_url(input_storage_name):
    url = f"https://{input_storage_name}.blob.core.windows.net/"
    return url
