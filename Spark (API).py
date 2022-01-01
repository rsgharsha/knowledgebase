# Databricks notebook source
# MAGIC %sh
# MAGIC ls -l /databricks/jars/*hadoop-hdfs*

# COMMAND ----------

df = spark.sql("""
select 
outerprojmo/12 as Projection_Year, 
variablevalue as Accum_Asset_Less_CV
from nfadev_mo_views.vw_modelvalue_get 
""")
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/hashm1

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

PostCalcDB_args_schema = StructType([
StructField("KeyValues",StringType(),False)
])

ModelValue_schema = StructType([
StructField("ID",StringType(),False),
StructField("Param_A",StringType(),False),
StructField("Param_B",StringType(),False),
StructField("Result",StringType(),False),
StructField("fk_PostCalcs_ID",StringType(),False),
StructField("BatchCode",StringType(),False)
])

PostCalcs_schema = StructType([
StructField("ID",StringType(),False),
StructField("Formula",StringType(),False),
StructField("Effective",StringType(),False)
])

PostCalcDB_args_df = spark.read.format("csv").option("header", "false").schema(PostCalcDB_args_schema).load("/FileStore/tables/hashm1/PostCalcDB_args.txt")
#display(PostCalcDB_args_df)

ModelValue_schema_df = spark.read.format("csv").option("header", "false").schema(ModelValue_schema).load("/FileStore/tables/hashm1/ModelValue.csv")
ModelValue_schema_cast_df = ModelValue_schema_df.selectExpr("CAST(Param_A AS INT)","CAST(Param_B AS INT)","CAST(Result AS INT)","CAST(fk_PostCalcs_ID AS INT)","CAST(BatchCode AS INT)")
ModelValue_schema_cast_filter_df = ModelValue_schema_cast_df.filter((ModelValue_schema_cast_df.Result == 0) & (ModelValue_schema_cast_df.BatchCode.isNull()))
#display(ModelValue_schema_cast_df)
#display(ModelValue_schema_cast_filter_df)

PostCalcs_schema_df = spark.read.format("csv").option("header", "false").schema(PostCalcs_schema).load("/FileStore/tables/hashm1/PostCalcs.csv")
PostCalcs_schema_cast_df = PostCalcs_schema_df.select(PostCalcs_schema_df.ID.cast(IntegerType()),col('Formula'),col('Effective').cast(IntegerType()))
PostCalcs_schema_cast_filter_df = PostCalcs_schema_cast_df.filter(PostCalcs_schema_cast_df.Effective == 1)
#display(PostCalcs_schema_cast_filter_df)

ModelValue_PostCalcs_join_cast_df = ModelValue_schema_cast_filter_df.join(PostCalcs_schema_cast_filter_df,ModelValue_schema_cast_filter_df.fk_PostCalcs_ID==PostCalcs_schema_cast_filter_df.ID,"inner")
#display(ModelValue_PostCalcs_join_cast_df)

def clsPostCalc(Param_A,Param_B,formula):
  
  if formula == "a+b":
    return Param_A + Param_B
  elif formula == "a-b":
    return Param_A - Param_B
  elif formula == "b-a":
    return Param_B - Param_A
  elif formula == "a/b":
    return Param_A / Param_B
  elif formula == "b/a":
    return Param_B / Param_A
  elif formula == "a^b":
    return Param_A ** Param_B
  elif formula == "b^a":
    return Param_B ** Param_A
    
  return Param_A * Param_B

clsPostCalc_udf = udf(clsPostCalc, LongType())

ClsPostCalc_df = ModelValue_PostCalcs_join_cast_df.select(ModelValue_PostCalcs_join_cast_df.ID,ModelValue_PostCalcs_join_cast_df.Param_A,ModelValue_PostCalcs_join_cast_df.Param_B,clsPostCalc_udf("Param_A","Param_B","Formula"),ModelValue_PostCalcs_join_cast_df.fk_PostCalcs_ID,ModelValue_PostCalcs_join_cast_df.BatchCode)
#display(ClsPostCalc_df)
ClsPostCalc_df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("/FileStore/tables/hashm1/op.csv")
#peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
#df_final.write.format("delta").mode("overwrite").save(deltaPath)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions.{when, _}
# MAGIC 
# MAGIC val ModelValue_schema = StructType(
# MAGIC StructField("ID",StringType,false)::
# MAGIC StructField("Param_A",IntegerType,false)::
# MAGIC StructField("Param_B",IntegerType,false)::
# MAGIC StructField("Result",IntegerType,false)::
# MAGIC StructField("fk_PostCalcs_ID",IntegerType,false)::
# MAGIC StructField("BatchCode",IntegerType,false):: Nil
# MAGIC )
# MAGIC 
# MAGIC val PostCalcs_schema = StructType(
# MAGIC StructField("ID",IntegerType,false)::
# MAGIC StructField("Formula",StringType,false)::
# MAGIC StructField("Effective",IntegerType,false):: Nil
# MAGIC )
# MAGIC 
# MAGIC val ModelValue_schema_df = spark.read.format("csv").option("header", "false").schema(ModelValue_schema).load("/FileStore/tables/hashm1/ModelValue.csv")
# MAGIC val ModelValue_schema_filter_df = ModelValue_schema_df.filter($"Result" === 0 and $"BatchCode".isNull).drop("ID")
# MAGIC 
# MAGIC val PostCalcs_schema_df = spark.read.format("csv").option("header", "false").schema(PostCalcs_schema).load("/FileStore/tables/hashm1/PostCalcs.csv")
# MAGIC val PostCalcs_schema_filter_df = PostCalcs_schema_df.filter($"Effective" === 1)
# MAGIC 
# MAGIC val ModelValue_PostCalcs_join_df = ModelValue_schema_filter_df.join(PostCalcs_schema_filter_df,ModelValue_schema_filter_df("fk_PostCalcs_ID") === PostCalcs_schema_filter_df("ID"),"inner")
# MAGIC 
# MAGIC val ModelValue_PostCalcs_join_calculation_df = ModelValue_PostCalcs_join_df.select(col("Formula"), when(col("Formula") === "a-b",col("Param_A") - col("Param_B")).when(col("Formula") === "b*a",col("Param_A") * col("Param_B")).otherwise("Unknown").alias("operator"))
# MAGIC display(ModelValue_PostCalcs_join_calculation_df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/jars/e69f3a55_f17d_4432_9312_005b5a8b5684-PostCalcDB.jar

# COMMAND ----------

spark2 = spark.getActiveSession()

# COMMAND ----------

spark2

# COMMAND ----------

# MAGIC %scala
# MAGIC import dbr1.HelloWorld
# MAGIC //new com.syed.postcalcdb.Program(spark).run()
# MAGIC val newH = new HelloWorld("suma");
# MAGIC newH

# COMMAND ----------

# MAGIC %scala
# MAGIC import dbr1.Person
# MAGIC //new com.syed.postcalcdb.Program(spark).run()
# MAGIC val suma = new Person("suma", 1);
# MAGIC suma.getId()
# MAGIC val vishnu = new Person("vishnu", 0);
# MAGIC suma.getName()

# COMMAND ----------

# MAGIC %scala
# MAGIC import dbr1.SparkDriver
# MAGIC //new com.syed.postcalcdb.Program(spark).run()
# MAGIC val sprkss = new SparkDriver(spark);
# MAGIC //sprkss.printSession()
# MAGIC print(sprkss)
# MAGIC //sprkss.printSession()

# COMMAND ----------

# MAGIC %sql SET spark.databricks.delta.formatCheck.enabled=false;

# COMMAND ----------

# MAGIC %scala
# MAGIC //spark.conf.set('spark.databricks.delta.formatCheck.enabled','false')
# MAGIC 
# MAGIC import dbr1.SparkSessionBuilder;
# MAGIC 
# MAGIC val sprkbldr = new SparkSessionBuilder();
# MAGIC print(sprkbldr)
# MAGIC sprkbldr.getCnt()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/hashm1/op.csv

# COMMAND ----------

import pandas as pd
properties_file = pd.read_csv("/dbfs/FileStore/tables/hashm1/PostCalcDB_args.txt", header='infer')
display(properties_file)
ModelValue_file = pd.read_csv("/dbfs/FileStore/tables/hashm1/op.csv/part-00000-tid-4674662184883096584-323cec95-bf5d-4fb1-b2a0-be96a2bb9bd7-45-1-c000.csv", header='infer')
display(ModelValue_file)


# COMMAND ----------

# MAGIC %scala
# MAGIC import dbr1.DBFScsvSparkSessionBuilder;
# MAGIC 
# MAGIC val dbfscsvsprkbldr = new DBFScsvSparkSessionBuilder("/FileStore/tables/hashm1/PostCalcDB_args.txt");
# MAGIC val cnt = dbfscsvsprkbldr.getCntModelValue()
# MAGIC print(cnt+":cnt is")

# COMMAND ----------

# MAGIC %scala
# MAGIC //dbfscsvsprkbldr.showContentModelValue()
# MAGIC //dbfscsvsprkbldr.getCntPostCalcs()
# MAGIC //dbfscsvsprkbldr.showContentPostCalcs()
# MAGIC //dbfscsvsprkbldr.getCntModelValue_PostCalcs_join()
# MAGIC //dbfscsvsprkbldr.showContentModelValue_PostCalcs_join()
# MAGIC //dbfscsvsprkbldr.getCntModelValue_PostCalcs()
# MAGIC //dbfscsvsprkbldr.showContentModelValue_PostCalcs()
# MAGIC dbfscsvsprkbldr.saveToDBFS()
# MAGIC //dbfscsvsprkbldr.propertiesMap()
# MAGIC //dbfscsvsprkbldr.printProp_file_path
# MAGIC //dbfscsvsprkbldr.showContentModelValueTest
# MAGIC //dbfscsvsprkbldr.showContentModelValueTest1

# COMMAND ----------

# MAGIC %scala
# MAGIC import dbr1.PostCalcsDBRewritten;
# MAGIC 
# MAGIC val pstCalcsDBRewritten = new PostCalcsDBRewritten("/FileStore/tables/hashm1/PostCalcDB_args.txt");
# MAGIC pstCalcsDBRewritten.runProcess()
# MAGIC //val cnt = pstCalcsDBRewritten.getCntModelValue()
# MAGIC //print(cnt+":cnt is")

# COMMAND ----------

print(type([str(i.KeyValues) for i in PostCalcDB_args_df.filter(PostCalcDB_args_df.KeyValues.like('ModelValueFileName%')).collect()]))
print([str(i.KeyValues) for i in PostCalcDB_args_df.filter(PostCalcDB_args_df.KeyValues.like('ModelValueFileName%')).collect()])
#ModelValueFileName = [int(i.colNameKey) for i in df.select("colNameKey").collect()]

# COMMAND ----------

word = "cake"
for i in word:
  first = i
  temp = word.replace(i,"")
  #print(temp)
  for j in temp:
    second = j
    tempJ = temp.replace(j,"")
    #print(temp)
    for k in tempJ:
      third = k
      tempK = tempJ.replace(k,"")
      fourth = tempK
      print(first,second,third,fourth)

# COMMAND ----------

word = "cake".split("")
print(word)
#temp = word.replace(i,"")
print(temp)
