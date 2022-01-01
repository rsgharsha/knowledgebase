# Databricks notebook source
df = spark.sql("""
select 
outerprojmo/12 as Projection_Year, 
variablevalue as Accum_Asset_Less_CV
from nfaprod_mo_views.vw_modelvalue_get 
""")
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/hashm1

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/jars/e69f3a55_f17d_4432_9312_005b5a8b5684-PostCalcDB.jar

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/hashm1/PostCalcDB_args.txt

# COMMAND ----------

spark.range(0, 1000, 1, 100)

# COMMAND ----------



# COMMAND ----------

spark2 = spark.getActiveSession()

# COMMAND ----------

spark2

# COMMAND ----------

# MAGIC %scala
# MAGIC new com.syed.postcalcdb.Program(spark).run()

# COMMAND ----------

for
