# Databricks notebook source
# DBTITLE 1,original query
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid



query1="SELECT SourceSystemID,SourceSchema,RunId,LoadId,ValuationDate,JobName,ModelName,RunDate,RunBy,LoadedBy,LoadDate,Purpose,Application,Notes,LOBName \
 FROM\
  (\
    SELECT\
      3 AS SourceSystemID,\
      R.SourceSchema,\
      'Combined' AS System,\
      R.RunId,\
      R.LoadId,\
      date_format(A.LoadDate,'MMMM dd yyyy hh:mm aa') AS ValuationDate,\
      A.JobName,\
     'Arborist' AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy,\
      A.LoadDate,\
      c.Purpose,\
      la.Application,c.Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_conformed.CombinedRunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunId\
      and R.SourceSystemID = 3\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
      Left join nfaprod_mo_ConformedRef.LOBs LB on A.LineOfBusinessId = LB.LiabPopID  \
      UNION ALL\
    SELECT\
      2 AS SourceSystemID,\
      R.SourceSchema,\
      'AXIS' AS System,\
      R.RunId,\
      R.LoadId,\
      concat(\
        substring(Cast(i.ValuationMonthDate as STRING), 1, 2),\
        '/',\
        substring(Cast(i.ValuationYearDate as STRING), 1, 4)\
      ) as ValuationDate,\
      i.JobName,\
      i.DatasetPathName AS ModelName,\
      i.BatchStartTs AS Rundate,\
      i.UserName AS RunBy,\
      COALESCE (l.NWIE, i.UserName) AS LoadedBy,\
      R.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_Axis.RunInfo AS i\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = i.RunId\
      LEFT JOIN nfaprod_mo_Axis.LoadInfo AS l ON l.LoadiD = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
     Left join nfaprod_mo_ConformedRef.LOBs LB on i.LineOfBusinessId = LB.LiabPopID \
  UNION ALL\
   SELECT\
      1 AS SourceSystemID,\
      R.SourceSchema,\
      'ALFA' AS System,\
      R.RunId,\
      R.LoadId,\
      A.ValuationDate,\
      CASE\
        WHEN length(rtrim(ProjectionDescription)) = 0 THEN AFD_FileName\
        ELSE ProjectionDescription\
      END AS JobName,\
      A.ProjectFileName AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy,\
      A.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName\
    FROM\
      nfaprod_mo_ALFA.RunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunID \
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
       Left join nfaprod_mo_ConformedRef.LOBs LB on A.pkLOB = LB.LiabPopID)"

# COMMAND ----------

df_data1=spark.sql(query1)
df_data1.count()

# COMMAND ----------

# DBTITLE 1,with sourcesystemid predicate
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid



query2="SELECT SourceSystemID,SourceSchema,RunId,LoadId,ValuationDate,JobName,ModelName,RunDate,RunBy,LoadedBy,LoadDate,Purpose,Application,Notes,LOBName \
 FROM\
  (\
    SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema,\
      'Combined' AS System,\
      R.RunId,\
      R.LoadId,\
      date_format(A.LoadDate,'MMMM dd yyyy hh:mm aa') AS ValuationDate,\
      A.JobName,\
     'Arborist' AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy,\
      A.LoadDate,\
      c.Purpose,\
      la.Application,c.Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_conformed.CombinedRunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunId\
      and R.SourceSystemID = 3\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
      Left join nfaprod_mo_ConformedRef.LOBs LB on A.LineOfBusinessId = LB.LiabPopID  \
      UNION ALL\
    SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema,\
      'AXIS' AS System,\
      R.RunId,\
      R.LoadId,\
      concat(\
        substring(Cast(i.ValuationMonthDate as STRING), 1, 2),\
        '/',\
        substring(Cast(i.ValuationYearDate as STRING), 1, 4)\
      ) as ValuationDate,\
      i.JobName,\
      i.DatasetPathName AS ModelName,\
      i.BatchStartTs AS Rundate,\
      i.UserName AS RunBy,\
      COALESCE (l.NWIE, i.UserName) AS LoadedBy,\
      R.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_Axis.RunInfo AS i\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = i.RunId and R.SourceSystemID = 2\
      LEFT JOIN nfaprod_mo_Axis.LoadInfo AS l ON l.LoadiD = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
     Left join nfaprod_mo_ConformedRef.LOBs LB on i.LineOfBusinessId = LB.LiabPopID \
  UNION ALL\
   SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema,\
      'ALFA' AS System,\
      R.RunId,\
      R.LoadId,\
      A.ValuationDate,\
      CASE\
        WHEN length(rtrim(ProjectionDescription)) = 0 THEN AFD_FileName\
        ELSE ProjectionDescription\
      END AS JobName,\
      A.ProjectFileName AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy,\
      A.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName\
    FROM\
      nfaprod_mo_ALFA.RunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunID and R.SourceSystemID = 1\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
       Left join nfaprod_mo_ConformedRef.LOBs LB on A.pkLOB = LB.LiabPopID)"

# COMMAND ----------

df_data2=spark.sql(query2)
df_data2.count()

# COMMAND ----------

query2  --->count (16293 no ssid ,16292 with ssid)
query3   ---->count( 53598 no ssid , 42834 with ssid)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nfaprod_mo_conformed.runinfo r
# MAGIC inner join nfaprod_mo_alfa.runinfo x  on r.RunId = x.runid 
# MAGIC where sourceSystemId = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nfaprod_mo_alfa.runinfo where RunId = 11111

# COMMAND ----------

# DBTITLE 1,missing loadids with sourcesystemid predicate
df_miss=df_data1.join(df_data2,"LoadId","leftanti")
df_miss.createTempView("df_miss")
display(spark.sql("select distinct SourceSystemId from df_miss"))

# COMMAND ----------

# DBTITLE 1,axis_runinfo duplicates
# MAGIC %sql select count(*),runid from nfaprod_mo_axis.runinfo group by runid having count(*)> 1

# COMMAND ----------

# DBTITLE 1,alfa_runinfo duplicates
# MAGIC %sql select count(*),runid from nfaprod_mo_alfa.runinfo group by runid having count(*)> 1

# COMMAND ----------

q1=" SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema as SourceSchema,\
      'Combined' AS System,\
      R.RunId as RunId,\
      R.LoadId as LoadId,\
      date_format(A.LoadDate,'MMMM dd yyyy hh:mm aa') AS ValuationDate,\
      A.JobName as JobName,\
     'Arborist' AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy AS LoadedBy,\
      A.LoadDate AS LoadDate,\
      c.Purpose AS Purpose,\
      la.Application AS Application,c.Notes AS Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_conformed.CombinedRunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunId\
      and R.SourceSystemID = 3\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
      Left join nfaprod_mo_ConformedRef.LOBs LB on A.LineOfBusinessId = LB.LiabPopID"

# COMMAND ----------

x1=spark.sql(q1)
x1.count()

# COMMAND ----------

x1.show()
x1.printSchema()

# COMMAND ----------

y = x1.groupBy('LoadId').agg(count('LoadId'))
y.printSchema()

# COMMAND ----------

import pyspark.sql.functions as f
x1.groupBy('LoadId').count().select("SourceSystemID","SourceSchema","RunId","LoadId","ValuationDate","JobName","ModelName","RunDate","RunBy","LoadedBy","LoadDate","Purpose","Application","Notes","LOBName", f.col('count').alias('n')).show()


# COMMAND ----------

y1=x1.select("SourceSystemID","SourceSchema","RunId","LoadId","ValuationDate","JobName","ModelName","RunDate","RunBy","LoadedBy","LoadDate","Purpose","Application","Notes","LOBName").groupby("LoadId").count().where("count > 1").sort("count",ascending=False)
y1.show()
y1.count()

# COMMAND ----------

q2=" SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema,\
      'AXIS' AS System,\
      R.RunId,\
      R.LoadId,\
      concat(\
        substring(Cast(i.ValuationMonthDate as STRING), 1, 2),\
        '/',\
        substring(Cast(i.ValuationYearDate as STRING), 1, 4)\
      ) as ValuationDate,\
      i.JobName,\
      i.DatasetPathName AS ModelName,\
      i.BatchStartTs AS Rundate,\
      i.UserName AS RunBy,\
      COALESCE (l.NWIE, i.UserName) AS LoadedBy,\
      R.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName \
    FROM\
      nfaprod_mo_Axis.RunInfo AS i\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = i.RunId and R.SourceSystemID = 2\
      LEFT JOIN nfaprod_mo_Axis.LoadInfo AS l ON l.LoadiD = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
     Left join nfaprod_mo_ConformedRef.LOBs LB on i.LineOfBusinessId = LB.LiabPopID"

# COMMAND ----------

x2=spark.sql(q2)
x2.count()

# COMMAND ----------

y2=x2.groupby("LoadId").count().where("count > 1").sort("count",ascending=False)
y2.show()
y2.count()

# COMMAND ----------

q3="SELECT\
      R.SourceSystemID AS SourceSystemID,\
      R.SourceSchema,\
      'ALFA' AS System,\
      R.RunId,\
      R.LoadId,\
      A.ValuationDate,\
      CASE\
        WHEN length(rtrim(ProjectionDescription)) = 0 THEN AFD_FileName\
        ELSE ProjectionDescription\
      END AS JobName,\
      A.ProjectFileName AS ModelName,\
      A.LoadDate AS RunDate,\
      A.LoadedBy AS RunBy,\
      A.LoadedBy,\
      A.LoadDate,\
      c.Purpose,\
      la.Application, c.Notes,LB.LOB as LOBName\
    FROM\
      nfaprod_mo_ALFA.RunInfo AS A\
      INNER JOIN nfaprod_mo_conformed.RunInfo AS R ON R.RunId = A.RunID and R.SourceSystemID = 1\
      Left Join nfaprod_mo_conformed.LoadClassifications C on c.LoadId = R.LoadID\
      Left Join nfaprod_mo_conformed.LoadApplications LA on LA.ClassificationID = C.ClassificationID\
       Left join nfaprod_mo_ConformedRef.LOBs LB on A.pkLOB = LB.LiabPopID"

# COMMAND ----------

x3=spark.sql(q3)
x3.count()

# COMMAND ----------

y3=x3.groupby("LoadId").count().where("count > 1").sort("count",ascending=False)
y3.show()
y3.count()

# COMMAND ----------

import pyspark.sql.functions as f
data = [
    ('a', 5),
    ('a', 8),
    ('a', 7),
    ('b', 1),
]
df = spark.createDataFrame(data, ["x", "y"])
df.groupBy('x').count().select('x', f.col('count').alias('n')).show()

# COMMAND ----------


