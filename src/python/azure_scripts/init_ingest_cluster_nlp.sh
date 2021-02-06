echo $DB_CLUSTER_ID
echo $DB_PYTHON_VERSION

echo 'installing python packages...'
/databricks/python/bin/pip install azure-storage-blob==12.7.1
/databricks/python/bin/pip install spark-nlp==2.7.2
echo 'Installation completed!'

echo 'Installed packages'
/databricks/python/bin/pip freeze
