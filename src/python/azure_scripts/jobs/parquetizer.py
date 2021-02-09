# -*- coding: utf-8 -*-
JOB_2 = """
{
    "name": "2-parquetizer",
    "new_cluster": {
      "spark_version": "6.4.x-scala2.11",
      "spark_conf": {
        "spark.hadoop.fs.azure.account.key.%INPUT_STORAGE_NAME%.blob.core.windows.net": "%INPUT_STORAGE_KEY%"
      },
      "node_type_id": "Standard_DS3_v2",
      "cluster_log_conf": {
        "dbfs": {
          "destination": "dbfs:/cluster-logs"
        }
      },
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
        "--jars",
        "/dbfs/FileStore/jars/maven/com/databricks/spark-xml_2.11-0.11.0.jar",
        "dbfs:/FileStore/code/parquetize_xml.py"
      ]
    },
    "max_concurrent_runs": 1
}
"""
