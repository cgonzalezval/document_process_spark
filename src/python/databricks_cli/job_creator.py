# -*- coding: utf-8 -*-
import subprocess
from src.python.azure_scripts.jobs import JOB_1, JOB_2, JOB_3, JOB_4, JOB_5, JOB_6, JOB_7
from src.python.app.constants import INPUT_STORAGE_KEY, INPUT_STORAGE_NAME


def execute(command: str):
    subprocess.run(command.split(" "))


def update_secrets_job(job):
    job = job.replace("%INPUT_STORAGE_NAME%", INPUT_STORAGE_NAME)
    job = job.replace("%INPUT_STORAGE_KEY%", INPUT_STORAGE_KEY)
    job = job.replace("\n", "")
    job = job.replace(" ", "")
    return job


def process_job(job):
    job = update_secrets_job(job)
    execute(f"databricks jobs create --json " + job)


list_jobs = [JOB_1, JOB_2, JOB_3, JOB_4, JOB_5, JOB_6, JOB_7]
for n, job in enumerate(list_jobs):
    print(f"Generating job num: {n}")
    process_job(job)
print("Finished!")
