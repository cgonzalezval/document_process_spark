# -*- coding: utf-8 -*-
import os
import subprocess
import zipfile


def execute(command: str):
    subprocess.run(command.split(" "))


def zipdir(path: str, ziph: zipfile.ZipFile):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".py"):
                ziph.write(os.path.join(root, file), file)


folders = ["code", "azure_scripts"]
for folder in folders:
    execute(f"dbfs rm -r dbfs:/FileStore/{folder}")
    execute(f"dbfs mkdirs dbfs:/FileStore/{folder}")

# Zip file with all the code to be distributed in the spark cluster
target_folder = "../app/"
target_zip_file = "../app/app.zip"
try:
    os.remove(target_zip_file)
except FileNotFoundError:
    pass
zipf = zipfile.ZipFile(target_zip_file, "w", zipfile.ZIP_DEFLATED)
zipdir(target_folder, zipf)
zipf.close()

for file in os.listdir(target_folder):
    if file.endswith(".py") or file.endswith(".zip"):
        input_file = os.path.join(target_folder, file)
        print(f"Uploading: {input_file}")
        execute(f"dbfs cp {input_file} dbfs:/FileStore/code/{file}")

target_folder = "../azure_scripts/"
for file in os.listdir(target_folder):
    if file.endswith(".py") or file.endswith(".zip") or file.endswith(".sh"):
        input_file = os.path.join(target_folder, file)
        print(f"Uploading: {input_file}")
        execute(f"dbfs cp {input_file} dbfs:/FileStore/azure_scripts/{file}")
print("Terminado!")
