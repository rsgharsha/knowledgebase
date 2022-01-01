# Databricks notebook source
a=[1,2,3,4,5]
b=[4,4,3,5,2]

# COMMAND ----------

from pyspark.sql.types import *
c= [(str(x),x+10) for x in range(1, 11)]
print(type(a[0]))
schema = StructType([
StructField("Values",StringType(),False)
])
df = spark.createDataFrame(a,schema)

# COMMAND ----------

RD_DF = spark.createDataFrame( 
      [
(85681), (85681) 
         ], 
    ['LoadId']
)

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
   StructField("colNameKey", IntegerType(), True),
   StructField("colNameValue", IntegerType(), True)
   ])
df=spark.createDataFrame([
  (x,x+10) for x in range(1, 11)],
   schema)
df.show()

# COMMAND ----------

print(a)
print(b)

# COMMAND ----------

spark.sql("show create table nfaprod_mo_views.vw_modelvalue_get").show(1,False)

# COMMAND ----------

dfr = spark.sql("""  SELECT 
                           lc.purpose,la.application,mvl.* ,ari.projectiondescription, ari.pklob
                    FROM nfaprod_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfaprod_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfaprod_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfaprod_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfaprod_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Production' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV' and 
                          projectiondescription =  'IAT Production CAUL' 
""")
dfr.show()

# COMMAND ----------

dfr = spark.sql("""  SELECT 
                           lc.purpose,la.application,mvl.* ,ari.projectiondescription, ari.pklob
                    FROM nfadev_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfadev_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfadev_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfadev_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfadev_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Production' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV' and 
                          projectiondescription =  'IAT Production CAUL' 
""")
dfr.show()

# COMMAND ----------

dfp=dfr.toPandas()

# COMMAND ----------

# DBTITLE 1,With all titles
import plotly.express as px
fig1 = px.line(dfp, x='outerprojmo', y='variablevalue', title='test graph1', color='modelgroupvalue')
fig1.show()

# COMMAND ----------

# DBTITLE 1,With all titles
import plotly.express as px
fig1 = px.line(dfp, x='outerprojmo', y='variablevalue', title='test graph1', color='modelgroupvalue')
fig1.show()

# COMMAND ----------

dfk=dfr.toPandas()

# COMMAND ----------

# DBTITLE 1,With all titles
from wordcloud import WordCloud
import plotly.express as px
#fig1 = px.WordCloud(dfp, x='outerprojmo', y='variablevalue', title='test graph1', color='modelgroupvalue')
#fig1.show()

# COMMAND ----------

# DBTITLE 1,w/o title;  w axes-lables
import plotly.express as px
fig1 = px.line(dfp, x='outerprojmo', y='variablevalue', color='modelgroupvalue')
fig1.update_xaxes(visible=True)
fig1.update_yaxes(visible=True)
fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_xaxes(showticklabels=True)
fig1.update_yaxes(showticklabels=True)
fig1.show()

# COMMAND ----------

# DBTITLE 1,w title;  w/o axes-lables
import plotly.express as px
fig1 = px.line(dfp, x='outerprojmo', y='variablevalue', title='test graph1', color='modelgroupvalue')
fig1.update_xaxes(visible=False)
fig1.update_yaxes(visible=False)
#fig1.update_xaxes(title='',visible=False,showticklabels=True)
#fig1.update_yaxes(title='',visible=True,showticklabels=True)
fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_xaxes(showticklabels=True)
fig1.update_yaxes(showticklabels=True)
fig1.show()

# COMMAND ----------

# DBTITLE 1,w/o title;  w/o axes-lables
import plotly.express as px
fig1 = px.line(dfp, x='outerprojmo', y='variablevalue', color='modelgroupvalue')
fig1.update_xaxes(title='',visible=False,showticklabels=True)
fig1.update_yaxes(title='',visible=True,showticklabels=True)
fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig1.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Yellow')
fig1.update_xaxes(showticklabels=True)
fig1.update_yaxes(showticklabels=True)
fig1.show()

# COMMAND ----------

import plotly.express as px
for i in range(1,5,2):
  print(i)
#fig = px.scatter(dff.loc[dff.VarName==variable], x='Percentile', y='Value', color="ProjectionDescription", facet_row='VarName', marginal_y="violin", marginal_x="box", trendline="ols", template="seaborn")  

# COMMAND ----------

import pandas as pd
import plotly.express as px

lst = [0,1,2,3,4,5]
val = [-3,10,20,15,35,10]
col = [10,10,20,10,20,10]

lst_na = ["lst_val","val_val","col_val"]

dict = {lst_na[0]:lst,lst_na[1]:val,lst_na[2]:col} 

print(lst)
print(val)
print(dict)

df = pd.DataFrame(dict, columns = lst_na)

df.head()

fig = px.line(df, x='lst_val', y='val_val', title='test graph', color='col_val')

#fig.update_xaxes(visible=False)
#fig.update_yaxes(visible=False)
fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Red')
fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Red')
fig.update_xaxes(showticklabels=True)
fig.update_yaxes(showticklabels=True)
fig.update_xaxes(title_text='')
fig.update_yaxes(title_text='')
#fig.update_yaxes(title_font=dict(size=18, family='Courier', color='crimson'))
fig.show()

# COMMAND ----------

import pandas as pd
import plotly.express as px

lst = [0,1,2,3,4,5]
val = [-3,10,20,15,35,10]
col = [10,10,20,10,20,10]

lst_na = ["lst_val","val_val","col_val"]

dict = {lst_na[0]:lst,lst_na[1]:val,lst_na[2]:col} 

print(lst)
print(val)
print(dict)

df = pd.DataFrame(dict, columns = lst_na)

df.head()

fig = px.line(df, x='lst_val', y='val_val', title='test graph', color='col_val')

#fig.update_xaxes(visible=False)
#fig.update_yaxes(visible=False)
fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGreen')
fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Red')
fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='Red')
fig.update_xaxes(showticklabels=True)
fig.update_yaxes(showticklabels=True)
#fig.update_xaxes(title_text='')
#fig.update_yaxes(title_text='')
#fig.update_yaxes(title_font=dict(size=18, family='Courier', color='crimson'))
fig.show()

# COMMAND ----------

import plotly.express as px

df = px.data.gapminder().query("continent=='Oceania'")
df.head(100)
fig = px.line(df, x="year", y="lifeExp", color='country')
fig.show()

# COMMAND ----------

two_arrya = [[0 for i in range(4)] for j in range(4)]
for i in range(0,4):
  for j in range(0,4):
    if i==1 and j ==2:
      two_arrya[i-1][j-1] = 1
    else:
      two_arrya[i][j] = 0
print(two_arrya) 

# COMMAND ----------

rows, cols = (4, 4) 
arr = [[0]*cols]*rows 
print(arr) 
arr[0][2] = 1
print(arr) 

# COMMAND ----------

x = [[0 for i in range(4)] for j in range(4)]
print(x)
x[0][2] = 1
print(x)

# COMMAND ----------

subscription_df = spark.createDataFrame([('a','y','2020-01-04'),('b','y','2020-02-02'),('a','n','2020-02-04'),('b','n','2020-03-15'),('a','y','2020-03-06')],['cid','is_p','st_date'])
subscription_df.show()
subscription_df.createOrReplaceTempView("subscription_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from subscription_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *,
# MAGIC   case
# MAGIC     when is_p = 'y' then st_date
# MAGIC   end as start_date
# MAGIC   ,lead(st_date) over(partition by cid order by st_date) as end_date
# MAGIC from
# MAGIC   subscription_tbl

# COMMAND ----------

def fibo(n):
  if n<0:
    print("invalid")
  elif n==0:
    print(n)
    return 0
  elif n==1:
    print(n)
    return 1
  else:
    return fibo(n-1)+fibo(n-2)
  
print(fibo(9))

# COMMAND ----------

ip_lst = [2,3,6,5,7,8,4,6,3,8]
k = 7
sort_ip_lst = sorted(ip_lst)
for sub_len in range (1,len(ip_lst)):
  for i in range():
    print(sub_len)

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, lower

schema_nfa = spark.sql("show databases like 'nfadev*'").select('databaseName').collect()
tables_all = []
for i in schema_nfa:
  sc = str(i.databaseName)
  spark.sql("use " + sc)
  tables_sc = spark.sql("show tables").select(concat(col("database"), lit("."), col("tableName")).alias("name"))
  tables = [str(row.name) for row in tables_sc.collect()] 
  for t in tables:
    tables_all.append(t)
    print(t)
print(len(tables_all))

# COMMAND ----------

cnts_dict = {}
fault_tables = {'nfadev_mo_conformed.view_modelval','nfadev_mo_conformed.view_modelval_0531'}
for i in range(len(tables_all)):
  if tables_all[i] in fault_tables:
    print(F"{i}-{tables_all[i]}:FAULT TABLE")
  else:
    try:
      df_table = spark.read.table(tables_all[i])
      cnt_i = df_table.count()
      cnts_dict[tables_all[i]] = cnt_i
      print(F"{i}-{tables_all[i]}:{cnt_i}")
    except Exception as e:
      print(F"{i}-{tables_all[i]}:table name with exception")

# COMMAND ----------

cnts_dict = {}
fault_tables = {'nfadev_mo_conformed.view_modelval','nfadev_mo_conformed.view_modelval_0531'}
for i in range(150):
  if tables_all[i] in fault_tables:
    print(F"{i}-{tables_all[i]}:FAULT TABLE")
  else:
    try:
      df_table = spark.read.table(tables_all[i])
      select_i = df_table.show()
      print("------------------=====================-------------------")
      print(F"{i}-{tables_all[i]}")
      print(select_i)
      print("------------------=====================-------------------")
    except Exception as e:
      print(F"table name with {e} exception is:{tables_all[i]}")
