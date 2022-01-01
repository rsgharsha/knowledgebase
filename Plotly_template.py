# Databricks notebook source
LoadIDs="'83315', '83316', '83317','83318','81029','81408','81409','82558'"
runid = "652525058"
Vname = 'BeckPVDE'
SName = 'Albert'
SID = 0

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import avg, col, round
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from pyspark.sql.functions import broadcast

spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInRead','CORRECTED')
vw_mv_get_df = spark.sql(""" Select V.LoadId, V.ModelVariableId VariableID, V.ModelGroupValue, V.SourceVarName VarName, V.CommonVarName, V.SrcSystemName, V.OuterScenarioId ScenarioID ,V.OuterScenarioDate Period,V.OuterProjMo,	V.InnerScenarioId,	V.InnerScenarioDate, V.InnerProjMo,	V.VariableValue Value, runid, outerscenarioId, SourceVarName, outerscenariodate, variablevalue
from nfaprod_mo_views.vw_modelvalue_get V 
WHERE V.LoadID in (""" + LoadIDs + """) or runid = """ + runid + """ 
""")
vw_mv_get_df.persist(StorageLevel.DISK_ONLY)

vw_mv_get_df.createOrReplaceTempView('vw_mv_get')

# COMMAND ----------

dfr = spark.sql("""  SELECT 
                           runid, sourcevarname, variablevalue,
                           row_number() over(partition by t.sourcevarname order by t.outerscenariodate) - 1 as Month,
                           case when lower(sourcevarname) = 'net premium - payout annuity' then 1 
                            when lower(sourcevarname) = 'expenses - first year' then 2 
                            when lower(sourcevarname) = 'expenses - renewal' then 3
                            when lower(sourcevarname) = 'net benefits - death' then 4 
                            when lower(sourcevarname) = 'net benefits - annuity' then 5
                            when lower(sourcevarname) = 'total net cashflow' then 6
                            when lower(sourcevarname) = 'change in actuarial reserve' then 7 
                            when lower(sourcevarname) = 'investment inc on cashflow & res' then 8
                            when lower(sourcevarname) = 'invest inc adjustment on cf & res' then 9
                            when lower(sourcevarname) = 'corporate tax on cashflow & res' then 10
                            when lower(sourcevarname) = 'profit on cashflow & reserve' then 11
                            when lower(sourcevarname) = 'change in required surplus' then 12
                            when lower(sourcevarname) = 'before tax income on req surplus' then 13
                            when lower(sourcevarname) = 'invest inc adjustment on req surp' then 14
                            when lower(sourcevarname) = 'total inv income on req surplus' then 15
                            when lower(sourcevarname) = 'corporate tax on required surplus' then 16
                            when lower(sourcevarname) = 'contribution to free surplus ctfs' then 17
                            else 18 
                           end as LineNo
                    FROM vw_mv_get As t
                    WHERE runid = 652525058 and
                          t.outerscenarioId = 0                     
                          and lower(t.sourcevarname) in ('net premium - payout annuity', 'expenses - first year', 'expenses - renewal', 'net benefits - death', 'net benefits - annuity', 'total net cashflow', 'change in actuarial reserve', 'investment inc on cashflow & res', 'invest inc adjustment on cf & res', 'corporate tax on cashflow & res', 'profit on cashflow & reserve', 'change in required surplus', 'before tax income on req surplus', 'invest inc adjustment on req surp', 'total inv income on req surplus', 'corporate tax on required surplus', 'contribution to free surplus ctfs')
                    GROUP BY t.runid,
                          t.sourcevarname,
                          t.outerscenariodate,
                          t.variablevalue
""")

# COMMAND ----------

RD_DF = spark.createDataFrame([(80687,'Stochastic,NB,WAL5H'),(80637,'Stochastic,NB,WAL7'),(80638,'Stochastic,NB,WAL9'),(80639,'Stochastic,NB,WAL10'),(80640,'Stochastic,NB,WAL13'),(81029,'Stochastic,FIA'),(81408,'Stochastic,FIA10%PrivateIncrease'),(81409,'Stochastic,20%CLO'),(82558,'Stochastic,FIA10%PrivateIncrease&20%CLO'),(83315,'Stochastic,FIAReinv'),(83316,'Stochastic,FIA10%PrivateIncreaseReinv'),(83317,'Stochastic,20%CLOReinv'),(83318,'Stochastic,FIA10%PrivateIncrease&20%CLOReinv')],['LoadId','ProjectionDescription'])
SC_DF = spark.createDataFrame([(80687,'Albert'),(80637,'Albert'),(80638,'Albert'),(80639,'Albert'),(80640,'Albert'),(81029,'Albert'),(81408,'Albert'),(81409,'Albert'),(82558,'Albert'),(83315,'Albert'),(83316,'Albert'),(83317,'Albert'),(83318,'Albert')],['LoadId','ScenarioName'])
RD_SC_DF = RD_DF.join(SC_DF, 'LoadId')
df_PVDE = vw_mv_get_df.filter(vw_mv_get_df.VarName.isin(Vname))
df_PVDE_J = df_PVDE.join(broadcast(RD_SC_DF),'LoadId')
df_PVDE_F = df_PVDE_J.filter((df_PVDE_J.ScenarioName.isin(SName)) & (df_PVDE_J.ScenarioID != SID ) & (df_PVDE_J.Period == df_PVDE_J.agg({'Period': 'min'}).collect()[0][0]))
df_PVDE_FD = df_PVDE_F.na.fill({'commonvarname': 'unknown'})
df_PVDE_F_P = df_PVDE_FD.toPandas().sort_values(by=['Period', 'ScenarioID'])
df_PVDE_F_P['Percentile'] = df_PVDE_F_P['LoadId VarName ScenarioID Value'.split()].groupby(by=['LoadId','VarName'])['Value'].rank(pct=True)

# COMMAND ----------

# DBTITLE 1,Bar Graph
dfr_fil=dfr.filter("LineNo >= 6 and LineNo <= 10 and Month between 100 and 150")
dfp_bar=dfr_fil.toPandas()
fig_bar = px.bar(dfp_bar, x='Month', y='variablevalue', color='sourcevarname', hover_data={'Month':False,'variablevalue':True,'sourcevarname':True})
fig_bar.update_layout(showlegend=True, plot_bgcolor="white")
fig_bar.update_layout(hovermode='x')
fig_bar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert the table to a bar chart
# MAGIC 
# MAGIC Under the table, click the chart <img src="http://docs.databricks.com/_static/images/notebooks/chart-button.png"/></a> icon. 
# MAGIC 
# MAGIC select bar and select plot options and select below options 
# MAGIC 1. column names as required (keys:Month, Series groupings:sourcevarname, values:variablevalue)
# MAGIC 1. type of aggregation (sum)
# MAGIC 1. type of bar (stacked)

# COMMAND ----------

# DBTITLE 1,above graph using databricks native 
display(dfr_fil)

# COMMAND ----------

# MAGIC %md 
# MAGIC <!--this text is commented out-->
# MAGIC <!--
# MAGIC comments
# MAGIC may
# MAGIC also
# MAGIC be
# MAGIC multiline
# MAGIC -->

# COMMAND ----------

# DBTITLE 1,Line Graph
dfr_fil_14 = dfr.filter((dfr.LineNo == '14') & ((dfr.Month > '5') & (dfr.Month < '250')))
dfp_line=dfr_fil_14.toPandas()
fig_line = px.line(dfp_line,x='Month',y='variablevalue',color='runid',hover_data={'Month':False,'runid':False,'variablevalue':True})
fig_line.update_layout(showlegend=True, plot_bgcolor="white")
fig_line.update_layout(hovermode="x",hoverlabel=dict(font_size=20,font_family="Rockwell"))
fig_line.update_layout(legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
fig_line.update_xaxes(title_text='',zeroline=True, zerolinewidth=2, zerolinecolor='Black', showgrid=False)
fig_line.update_yaxes(title_text='',zeroline=True, zerolinewidth=2, zerolinecolor='Black', showgrid=True)
fig_line.show()

# COMMAND ----------

# DBTITLE 1,above graph using databricks native 
display(dfr_fil_14)

# COMMAND ----------

# DBTITLE 1,Scatter Plot
def metric_fmt(x):
  if x.VarName == 'BeckPVDE': 
    return '${:,.2f}'
  elif x.VarName in ['UndiscountedROA', 'Becker_ROTC']:
    return '{:.2%}'
  else:
    return '{:.1f}'
  
pdf_DE_CTE = df_PVDE_F_P[(df_PVDE_F_P.Percentile <= 0.05)].sort_values(by=['Percentile'],ascending=False)
pdf_DE_CTE_mean = pdf_DE_CTE.groupby(['LoadId', 'ProjectionDescription','VarName','ScenarioName']).agg({'Value': 'mean'}).reset_index()
pdf_DE_CTE_mean.Value = pdf_DE_CTE_mean.apply(lambda x: metric_fmt(x).format(x.Value), axis=1)
pdf_DE_CTE_mean.VarName = ['BeckPVDE_CTE95']*len(pdf_DE_CTE_mean)
pdf_cte = pdf_DE_CTE_mean[['LoadId','ProjectionDescription','Value']]
pdf_cte = pdf_cte.rename(columns={'Value':'CTE95'})
pdf_DE_mean = df_PVDE_F_P.groupby(['LoadId', 'ProjectionDescription','VarName','ScenarioName']).agg({'Value': 'mean'}).reset_index()
pdf_DE_mean.Value = pdf_DE_mean.apply(lambda x: metric_fmt(x).format(x.Value), axis=1)
pdf_DE_mean.VarName = ['BeckPVDE_avg']*len(pdf_DE_mean)
pdf_pvde = pdf_DE_mean[['LoadId','Value']]
pdf_pvde = pdf_pvde.rename(columns={'Value':'PVDE'})

pdf_cvurve = pdf_cte.merge(pdf_pvde, left_on='LoadId', right_on='LoadId')
fig = px.scatter(pdf_cvurve, x="CTE95", y="PVDE",  color="ProjectionDescription",
           hover_name="ProjectionDescription", log_x=False, size_max=60)
fig.show()

# COMMAND ----------

# DBTITLE 1,S - Curve
def scurve(dff, variable, iscash=False):
#   dff.loc[dff.VarName==variable,'Value'] =  dff.loc[dff.VarName==variable,'Value']/1000000
  fig = px.scatter(dff.loc[dff.VarName==variable], x='Percentile', y='Value', color="ProjectionDescription", facet_row='VarName', marginal_y="violin", marginal_x="box", trendline="ols", template="seaborn")
  fig.update_layout(yaxis_tickformat="$.0f" if iscash else ".2%", xaxis_tickformat=".2%")
  return fig
scurve(df_PVDE_F_P,'BeckPVDE', iscash=True)

# COMMAND ----------

# DBTITLE 1,3D Surface Graph
pivot_Tbl = dfr.groupby("runid", "sourcevarname", "LineNo").pivot("Month").agg(avg("variablevalue")).orderBy(*('runid', 'LineNo'), ascending=True)
pivot_Tbl_Pandas = pivot_Tbl.toPandas()
c = pivot_Tbl_Pandas.shape[1]-3
x = np.linspace(0,594,c)
df = pivot_Tbl_Pandas.drop(['runid', 'sourcevarname', 'LineNo'], axis=1)

fig_3dsurface = go.Figure(data=[go.Surface(z=df.values,x=x, y=StatInc_Tbl_Pandas.LineNo)])
fig_3dsurface.show()

# COMMAND ----------

vw_mv_get_df.unpersist()

# COMMAND ----------


