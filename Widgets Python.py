# Databricks notebook source
# MAGIC %md ## Text Widgets

# COMMAND ----------

# DBTITLE 1,Create
Loadid_Text = dbutils.widgets.text("LoadID_Text","")

# COMMAND ----------

# DBTITLE 1,Access
Loadid_Text = dbutils.widgets.get("LoadID_Text")
print(Loadid_Text)
print(type(Loadid_Text))

# COMMAND ----------

# MAGIC %md ## Dropdown Widgets

# COMMAND ----------

# DBTITLE 1,Create
dbutils.widgets.dropdown("LoadID_DropDown", "default", ['default']+[str(x) for x in range(1, 11)])

# COMMAND ----------

# DBTITLE 1,Access
LoadId_Dropdown=dbutils.widgets.get("LoadID_DropDown")
print(LoadId_Dropdown)
print(type(LoadId_Dropdown))

# COMMAND ----------

# MAGIC %md ## Multi Select Widgets

# COMMAND ----------

# DBTITLE 1,Create
dbutils.widgets.multiselect("LoadID_MultiSelect", "default", ['default']+[str(x) for x in range(1, 11)])

# COMMAND ----------

# DBTITLE 1,Access
Loadid_Multiselect=dbutils.widgets.get("LoadID_MultiSelect")
print(Loadid_Multiselect)
print(type(Loadid_Multiselect))

# COMMAND ----------

# MAGIC %md ## Combo Box Widgets

# COMMAND ----------

# DBTITLE 1,Create
dbutils.widgets.combobox("LoadID_ComboBox", "default", ['default']+[str(x) for x in range(1, 11)])

# COMMAND ----------

# DBTITLE 1,Access
LoadID_Combobox=dbutils.widgets.get("LoadID_ComboBox")
print(LoadID_Combobox)
print(type(LoadID_Combobox))

# COMMAND ----------

# DBTITLE 1,Remove
#dbutils.widgets.remove("LoadID_Text")

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ##Widgets from spark dataframe

# COMMAND ----------

# A sample dataframe is created just for tutorial purpose, but in real the spark dataframe comes from datalake
from pyspark.sql.types import *
schema = StructType([
   StructField("colNameKey", StringType(), True),
   StructField("colNameValue", IntegerType(), True)
   ])
df=spark.createDataFrame([
  (str(x),x+10) for x in range(1, 11)],
   schema)
df.show()

# COMMAND ----------

ColNameKey_List = [int(i.colNameKey) for i in df.select("colNameKey").collect()]
print(ColNameKey_List)
#use this list for creating different widgets as show above
