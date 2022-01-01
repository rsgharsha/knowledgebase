# Databricks notebook source
df = spark.range(1, 5)
df.createOrReplaceTempView("test")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test

# COMMAND ----------

from pyspark.sql.types import LongType
def squared_typed(s):
  return s * s

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
squared_udf = udf(squared_typed, LongType())
df.select("id",squared_udf("id")).show()

# COMMAND ----------

from pyspark.sql.functions import udf
@udf("long")
def squared_udf_annotation(s):
  return s * s

df.select("id", squared_udf_annotation("id").alias("id_squared_anotation")).show()

# COMMAND ----------

spark.udf.register("squaredWithPython", squared_typed, LongType())

# COMMAND ----------

# MAGIC %sql select id, squaredWithPython(id) as id_squared from test

# COMMAND ----------

search_word = "sesha"
main_string1 = "IAT Model Testing IUL/EIUL Accum Self"
main_string2 = "IAT Model Testing IUL/EIUL Prot Self"
main_string3 = "IAT Model Testing IUL/EIUL Classic Self"
print(search_word in main_string1)
#print(type())

# COMMAND ----------

print(type((' ' + 'brown' + ' ')))

# COMMAND ----------

def contains_word(s, w):
    return (' ' + w + ' ') in (' ' + s + ' ')

contains_word('the quick brown fox', 'brown')  # True
contains_word('the quick brown fox', 'row')    # False

# COMMAND ----------

from pyspark.sql import *
ul5_sample1 = Row(projectdescription='UL 5 IAT')
ul5_sample2 = Row(projectdescription='UL 5 IAT Production')
iul_sample1 = Row(projectdescription='IAT Model Testing IUL/EIUL Accum Self')
iul_sample2 = Row(projectdescription='IAT Model Testing IUL/EIUL Prot Self')
iul_sample3 = Row(projectdescription='IAT Model Testing IUL/EIUL Classic Self')
row_list = [ul5_sample1, ul5_sample2, iul_sample1, iul_sample2, iul_sample3]
df_cr = spark.createDataFrame(row_list)
df_cr.show()
