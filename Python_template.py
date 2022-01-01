# Databricks notebook source
# DBTITLE 1,Scope
# MAGIC %md This template includes reading data from Nationwide acturial data lake, input user defined values using Widgets and Variables, staging temporary tables, calculating metrics, transformations, joins, pivoting and finally visualizing end results, using Pyhton and pyspark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach the notebook to the cluster and run all commands in the notebook
# MAGIC 
# MAGIC 1. Return to this notebook. 
# MAGIC 1. In the notebook menu bar, select **<img src="http://docs.databricks.com/_static/images/notebooks/detached.png"/></a> > NFActurial_ModelOutput_Prod_RO**.
# MAGIC 1. When the cluster changes from <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>, click **<img src="http://docs.databricks.com/_static/images/notebooks/run-all.png"/></a> Run All**.

# COMMAND ----------

# DBTITLE 1,Widgets
#Remove a widget

dbutils.widgets.remove("sourcevarname")


#create and access LoadId from widgets dynamically
Loadid = dbutils.widgets.text("LoadID","")
Loadid = dbutils.widgets.get("LoadID")

# COMMAND ----------

# MAGIC %md [for detailed widgets notebook with samples: Right click here to open it in new tab](https://nationwide-prd.cloud.databricks.com/#notebook/765175/command/842043)

# COMMAND ----------

# DBTITLE 1,Variables
sourcevarname = 'LapseGMDBAVZero'

sourcevarname = dbutils.widgets.text("sourcevarname","LapseGMDBAVZero")


# COMMAND ----------

# MAGIC %md
# MAGIC **Python String formatting:**
# MAGIC You can use format() after the String to do simple positional formatting of variable substitutions.
# MAGIC 1. {0} is first position
# MAGIC 2. {1} is second position and so on.
# MAGIC 
# MAGIC         y = "The first value is one = {0} and the second value is two = {1}".format(1, 2)
# MAGIC 
# MAGIC You can also refer to your variable substitutions by name and use them in any order you want.
# MAGIC 
# MAGIC     x = "The first value is one = {one} and the second value is two = {second}".format(one=1, second=2)
# MAGIC 
# MAGIC **Python Quotes:**
# MAGIC The most common use of single and double quotes is to represent strings by enclosing a series of characters. As shown in the code below, we create these two strings using single and double quotes, respectively.
# MAGIC 
# MAGIC      quotes_single = 'a_string'
# MAGIC      quotes_double = "a_string"
# MAGIC      quotes_single == quotes_double
# MAGIC 
# MAGIC Triple Quotes: Triple quotes is used to represent a multi-line string.
# MAGIC 
# MAGIC     print("""The first value is one = {one} and 
# MAGIC               the second value is two = {second}""".format(one=1, second=2))

# COMMAND ----------

# DBTITLE 1,Reading Data from Data Lake
#Create a dataframe from reading a delta table
#You access data in Delta tables by specifying the table name ("db.TableName")

df_table = spark.read.table('Nfaprod_Mo_Conformed.Runinfo')

#You can also read the table to dataframe using a SQL like syntax

df_table = spark.sql("SELECT * FROM Nfaprod_Mo_Conformed.Runinfo")
df_table = sql("SELECT * FROM Nfaprod_Mo_Conformed.Runinfo")    # You can also just use SQL()

#Widgets is made dynamic, but access the values from the widgets and variables
# getArgument() is used to get the values of the widget.
staging_spark_dataframe = spark.sql(""" select * 
                                        from nfaprod_mo_views.vw_modelvalue_get as mvl
                                        where mvl.loadid = {0} and
                                        mvl.sourcevarname = '{1}'
                                    """.format(getArgument("LoadID"), getArgument("sourcevarname")))

alfa_runinfo_dataframe = spark.sql("""select * from nfaprod_mo_ALFA.RunInfo""")

# COMMAND ----------

# DBTITLE 1,Using Variables in string
# getArgument() is used to get the values of the widget.
#The Variables in string format can be directly used in the string value by using + <variable> + with in the string statement.
LoadIDs = getArgument("LoadIDs")
Srinivasdf = spark.sql("""select * from NFAPROD_MO_VIEWS.VW_MODELVALUE_GET where loadid in ("""+LoadIDs+""") """)
Srinivasdf.show(5)

#The Variables in string format can be directly used in the string value by using + <variable> + with in the string statement.
ALFALoadIDs ="101519, 101520, 101521, 101522"
spark.sql("""select * from NFAPROD_MO_VIEWS.VW_MODELVALUE_GET where loadid in ("""+ALFALoadIDs+""") """).show(5)

# COMMAND ----------

# DBTITLE 1,Use variables as widgets in Pyspark

spark.sql(""" select RunId,Loadid,ValuationDate,sourcevarname,Modelvariableid  from nfaprod_mo_views.vw_modelvalue_get as mvl
                                        where mvl.loadid = {0} and
                                        mvl.sourcevarname = '{1}'
                                    """.format(getArgument("LoadID"), getArgument("sourcevarname"))).show(5)

#Set the Widget value to a Variable and use in the print statement
LoadIdPrint = getArgument("LoadID")
print("LoadId is " + LoadIdPrint)

#Using getArgument() as widget value in the pyspark code
print("SourceVariableName is " + getArgument("sourcevarname"))

# COMMAND ----------

# DBTITLE 1,Access data using SQL syntax
# Register table so it is accessible via SQL Context
df_table.createOrReplaceTempView("df_table")

spark.sql("select * from df_table").show()

# COMMAND ----------

# DBTITLE 1,Display
# Display function can be used to view the first 1000 records of the dataframe
display(df_table) 


# Dataframe operations can also be used inside the Display function
display(df_table.select('LoadId').orderBy("LoadId"))

#Show function can also be used to display the data

df_table.show(100,truncate=True, vertical=False)
# If number of records is not specified by default is displays only 20 records
# truncate – If set to True, truncate strings longer than 20 chars by default. If set to a number greater than one, truncates long strings to length truncate and align cells right.
#vertical – If set to True, print output rows vertically (one line per column value)

# Take funtion can also be used to display the data for specific number
df_table.take(20) # This will display only first 20 records

# Display the first row
df_table.first()



# COMMAND ----------

# DBTITLE 1,Drop column
# drop column
drop_spark_dataframe = staging_spark_dataframe.drop('Valuationdate')

# COMMAND ----------

# DBTITLE 1,PySpark Join Types
# MAGIC %md
# MAGIC PySpark Join is used to join two or more DataFrames, It supports all basic join operations available in traditional SQL.
# MAGIC 
# MAGIC **join()** operation takes parameters as below and returns DataFrame.
# MAGIC 
# MAGIC param other: Right side of the join
# MAGIC param on: a string for the join column name
# MAGIC param how: default inner. Must be one of 
# MAGIC **inner, cross, outer,full, full_outer, left, left_outer, right, right_outer,left_semi, and left_anti**
# MAGIC 
# MAGIC Inner Join - Inner join is the default join in PySpark and it’s mostly used. This joins two datasets on key columns, where keys don’t match the rows get dropped from both datasets
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'inner')
# MAGIC 
# MAGIC Outer Join - Outer a.k.a full, fullouter join returns all rows from both datasets, where join expression doesn’t match it returns null on respective record columns.
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'full_outer')
# MAGIC 
# MAGIC Left Join - Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'left')
# MAGIC 
# MAGIC Right Join - Right a.k.a Rightouter join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset, when join expression doesn’t match, it assigns null for that record and drops records from left where match not found.
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'right_outer')
# MAGIC 
# MAGIC Left Semi Join - 
# MAGIC leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets.
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'left_semi')
# MAGIC 
# MAGIC Left Anti Join - leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.
# MAGIC 
# MAGIC df1.join(df2, df1.Column1 == df2.Column2, 'left_anti')

# COMMAND ----------

# DBTITLE 1,Join the tables using DataFrame API
#joined_spark_dataframe = drop_spark_dataframe.join(alfa_runinfo_dataframe,staging_spark_dataframe.RunId == alfa_runinfo_dataframe.RunID,'inner')
joined_spark_dataframe = drop_spark_dataframe.join(alfa_runinfo_dataframe,'RunID','inner')
joined_spark_dataframe.show()

#Full Outer Join
joined_spark_dataframe = drop_spark_dataframe.join(alfa_runinfo_dataframe,'RunID','full')
joined_spark_dataframe.show()

#Left Anti Join 
joined_spark_dataframe = drop_spark_dataframe.join(alfa_runinfo_dataframe,'RunID','left_anti')
joined_spark_dataframe.show()

#Left Semi Join 
joined_spark_dataframe = drop_spark_dataframe.join(alfa_runinfo_dataframe,'RunID','left_semi')
joined_spark_dataframe.show()


# COMMAND ----------

# DBTITLE 1,Filter
# Filters rows using the given condition
df_table.filter('LoadId'=10002).show()

#where() is an alias for filter()
df_table.where('LoadId'=10002).show()

filter_spark_dataframe = joined_spark_dataframe.filter(joined_spark_dataframe.CodeID >= 10)

# COMMAND ----------

# DBTITLE 1,Aggregation of the data
from pyspark.sql.functions import sum
aggregate_spark_dataframe = transformation_spark_dataframe.groupby(transformation_spark_dataframe.RunId,'Loadid',transformation_spark_dataframe.ValuationDate,'SourceVarname').agg(sum('Variablevalue')).alias('VariableValue')

#GroupBy() - Groups the DataFrame using the specified columns, so we can run aggregation on them.
df_table.groupBy("LoadId").count().show()

df_table.groupBy("LoadId").avg("RunId").show()

# COMMAND ----------

# MAGIC %md [For Aggregation functions details: Right click here to open it in new tab](https://nationwide-prd.cloud.databricks.com/#notebook/1026271)

# COMMAND ----------

# DBTITLE 1,OrderBy 
#OrderBy() - Returns a data sorted by the specified columns
df_table.orderBy(df_table.LoadId.desc())
df_table.orderBy("LoadId", ascending=False)  #ascending – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.

df_table.orderBy(["LoadId","RunId"])  #cols – list of Column or column names to sort by 

# COMMAND ----------

# DBTITLE 1,Calculation
calculation_spark_dataframe = filter_spark_dataframe.withColumn("VariableValue_calc",filter_spark_dataframe.VariableValue + 5)

# COMMAND ----------

# DBTITLE 1,Transformation
from pyspark.sql.functions import when
transformation_spark_dataframe = calculation_spark_dataframe.withColumn("ProjectionDescription_transform",when(calculation_spark_dataframe.Modelgroupvalue == 'A','self').when(calculation_spark_dataframe.Modelgroupvalue == 'B' ,'Lapse').otherwise(calculation_spark_dataframe.Modelgroupvalue))

# COMMAND ----------

# DBTITLE 1,Pivot
pivot_spark_dataframe = aggregate_spark_dataframe.groupby('RunId').pivot('ValuationDate').agg(sum(aggregate_spark_dataframe.ValuationDate))

# COMMAND ----------

# DBTITLE 1,Plotting 
# MAGIC %md [For Visualization Demo: Right click here to open it in new tab](https://nationwide-prd.cloud.databricks.com/#notebook/993716/command/1036756)
