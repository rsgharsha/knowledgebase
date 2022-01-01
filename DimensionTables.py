# Databricks notebook source
schema_nfaprod = spark.sql("show databases like 'nfaprod_mo_*'").select('namespace').collect()
print(schema_nfaprod)

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, lower
#schema_nfadev = spark.sql("show databases like 'nfadev_mo_*'").select('namespace').collect()
schema_nfaprod = spark.sql("show databases like 'nfaprod_mo_*'").select('namespace').collect()
tables_all = []
for i in schema_nfaprod:
  sc = str(i.namespace)
  spark.sql("use " + sc)
  tables_sc = spark.sql("show tables").select(concat(col("database"), lit("."), col("tableName")).alias("name"))
  tables = [str(row.name) for row in tables_sc.collect()] 
  for t in tables:
    tables_all.append(t)
tables_not = ["nfadev_mo_alfa.runinfo_test"]
#for i in tables_not:
 # tables_all.remove(i)
for i in tables_all:
  tables_exist = spark.sql("describe " + i).select('col_name').filter(lower(col("col_name")).isin(["loadid","valuationdate"]))
  if tables_exist.count() > 0:
    tables_exist.show()
    print(i)

# COMMAND ----------

table = spark.sql("describe nfadev_mo_conformedwork.sourcesystem")
#table.show()
if table.count() > 0:
  table.show()

# COMMAND ----------

schema_nfadev = spark.sql("show databases like 'nfaprod_mo_*'").select('databaseName').collect()

# COMMAND ----------

schema_nfadev = spark.sql("show databases like 'nfaprod_mo_*'")
schema_nfadev.show()
