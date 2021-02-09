# -*- coding: utf-8 -*-
JOB_5 = """
{
    "name": "5-frequent_words",
    "new_cluster": {
      "spark_version": "6.4.x-scala2.11",
      "spark_conf": {
        "spark.hadoop.fs.azure.account.key.%INPUT_STORAGE_NAME%.blob.core.windows.net": "%INPUT_STORAGE_KEY%"
      },
      "node_type_id": "Standard_DS3_v2",
      "enable_elastic_disk": true,
      "init_scripts": [
        {
          "dbfs": {
            "destination": "dbfs:/FileStore/azure_scripts/init_ingest_cluster.sh"
          }
        }
      ],
      "num_workers": 2
    },
    "email_notifications": {},
    "timeout_seconds": 0,
    "spark_submit_task": {
      "parameters": [
        "--py-files",
        "dbfs:/FileStore/code/app.zip",
        "dbfs:/FileStore/code/frequent_words.py"
      ]
    },
    "max_concurrent_runs": 1
}

"""