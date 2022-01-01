# Databricks notebook source
display(dbutils.fs.ls('dbfs:/FileStore/tables/init-3.sh'))

# COMMAND ----------

open('/dbfs/FileStore/tables/init-3.sh',mode='r')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/init-3.sh')

# COMMAND ----------

import os
print(os.popen('sh /FileStore/tables/init-3.sh'))

# COMMAND ----------

# MAGIC %sh
# MAGIC sh /dbfs/FileStore/tables/init.sh
