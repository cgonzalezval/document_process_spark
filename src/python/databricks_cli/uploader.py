# -*- coding: utf-8 -*-
import os
import subprocess
from utils import zip_app, get_app_path


def execute(command: str):
    subprocess.run(command.split(" "))


# Crete a zip with the app to be distributed
zip_app()

folder_code = "code"
folder_azure_scritps = "azure_scripts"
folder_resources = "resources"
folders = [folder_code, folder_resources, folder_azure_scritps]
for folder in folders:
    execute(f"dbfs rm -r dbfs:/FileStore/{folder}")
    execute(f"dbfs mkdirs dbfs:/FileStore/{folder}")


target_folder = get_app_path()
for file in os.listdir(target_folder):
    if file.endswith(".py") or file.endswith(".zip"):
        input_file = os.path.join(target_folder, file)
        print(f"Uploading: {input_file}")
        execute(f"dbfs cp {input_file} dbfs:/FileStore/{folder_code}/{file}")

target_folder = "../azure_scripts/"
for file in os.listdir(target_folder):
    if file.endswith(".py") or file.endswith(".zip") or file.endswith(".sh"):
        input_file = os.path.join(target_folder, file)
        print(f"Uploading: {input_file}")
        execute(f"dbfs cp {input_file} dbfs:/FileStore/{folder_azure_scritps}/{file}")

target_folder = "../../resources/"
for file in os.listdir(target_folder):
    if file.endswith(".jar"):
        input_file = os.path.join(target_folder, file)
        print(f"Uploading: {input_file}")
        execute(f"dbfs cp {input_file} dbfs:/FileStore/{folder_resources}/{file}")

print("Terminado!")
