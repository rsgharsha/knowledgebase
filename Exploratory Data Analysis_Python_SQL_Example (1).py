# Databricks notebook source
# MAGIC %md 
# MAGIC # Exploratory Data Analysis and Visualization using Databricks

# COMMAND ----------

# MAGIC %md 
# MAGIC ## The Basics 
# MAGIC Databricks notebooks are based on the Jupyter Notebook. Text is formatted using [markdown](https://commonmark.org/). 
# MAGIC 
# MAGIC The plots in this notebook are made using [plotly](https://plot.ly/python/) for python. Plotly's Python graphing library makes interactive, publication-quality graphs. This library has over 30 chart types. For this report specifically, I am using the express module in plotly, which allows for the the easy creation of figures. The library is imported as: 

# COMMAND ----------

import plotly.express as px
import plotly.graph_objs as go

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from nfaprod_mo_conformed.modelvalue

# COMMAND ----------

# MAGIC %md
# MAGIC To work with databricks data in python, I use the sql module in the pyspark package. 

# COMMAND ----------

from pyspark.sql import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data
# MAGIC The query below grabs data from ModelOutput for two load ids from the databrick's cluster ```nfaprod_mo_conformed```.

# COMMAND ----------

query = """
  select
    LoadId,
    OuterScenarioId as Scenario,
    cvar.VariableName as Variable,
    rvar.VariableName as RawVariable,
    pcs.PostCalcEqu as Equation,
    ProjYear, 
    Cast(VariableValue as float) as Value
  from nfaprod_mo_conformed.modelvalue mval
  join nfaprod_mo_conformed.modelvariablexref mvx 
    on mval.ModelVariableId = mvx.SourceModelVariableId
    and mvx.EndDate >= current_date()
  join nfaprod_mo_conformed.modelvariable rvar 
    on mval.ModelVariableId = rvar.ModelVariableId
    and rvar.EndDate >= current_date()
  join nfaprod_mo_conformed.modelvariable cvar 
    on mvx.CommonModelVariableId = cvar.ModelVariableId
    and cvar.EndDate >= current_date()
  left join nfaprod_mo_conformed.postcalcs pcs 
    on rvar.VariableName = pcs.PostCalcVar
    and rvar.SourceSystemID = pcs.SourceSystemID
    and pcs.EndDate >= current_date()
  where mval.LoadId = 41559
"""

# COMMAND ----------

# MAGIC %md
# MAGIC The query is run and staged using the command ```spark.sql```

# COMMAND ----------

# MAGIC %%time
# MAGIC df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotting Data
# MAGIC In this section, I'll show the common charts shown in actuarial reports, make some comments on them, and demonstrate that notebooksa are an easy, transparent, and reproducible way share information. 

# COMMAND ----------

# MAGIC %md ### Main Pricing Metrics
# MAGIC Let's look at the main metrics for this run using scurves and boxplots. 

# COMMAND ----------

# MAGIC %md
# MAGIC First, I create a list of the main pricing metrics. I then filter the data from ```df``` and convert it to a ```pandas``` dataframe. You can read more about pandas [here](https://pandas.pydata.org/). 

# COMMAND ----------

main_metrics = ['Becker_PVDE', 'Becker_ROTC', 'Breakeven Month', 'Expected Policy Life', 'UndiscountedROA' ]
dff = df.filter(df.Variable.isin(main_metrics)).toPandas().sort_values(by=['Variable', 'Value'])

# COMMAND ----------

# MAGIC %md
# MAGIC To make s-curves, I need to rank the scenarios along their values for each variable. I call the new column 'Percentile'. In this one line of code, the scenarios are ranked by value for each Load Id and Variable Name. The ranking is expressed as a percentile (e.g. from 0 to 1). 

# COMMAND ----------

dff['Percentile'] = dff['LoadId Variable Scenario Value'.split()].groupby(by=['LoadId','Variable'])['Value'].rank(pct=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The plotly express library is a quick (one-line) way to graph charts. You lose out on a lot of the advanced formatting and options that ```plotly``` has. However, it allows you to visualize data very quickly. 
# MAGIC 
# MAGIC Below, I create a s-curve of PVDE and ROTC with a marinal violin plot. A violin plot is a box plot which shows the density of the points plotted, instead of a uniform box. To simplify things, I use a function that varies on the variable plotted and the y-axis format to plot the charts. 

# COMMAND ----------

def scurve(variable, iscash=False):
  fig = px.scatter(dff.loc[dff.Variable==variable], x='Percentile', y='Value', color="Variable", facet_row='Variable', marginal_y="violin", marginal_x="box", trendline="ols", template="seaborn")
  fig.update_layout(yaxis_tickformat="$.0f" if iscash else ".2%", xaxis_tickformat=".2%")
  return fig

# COMMAND ----------

scurve('Becker_PVDE', iscash=True)

# COMMAND ----------

scurve('Becker_ROTC', iscash=False)

# COMMAND ----------

def metric_fmt(x):
  if x.Variable == 'Becker_PVDE': 
    return '${:,.2f}'
  elif x.Variable in ['UndiscountedROA', 'Becker_ROTC']:
    return '{:.2%}'
  else:
    return '{:.1f}'

global_values = dff.loc[dff.Scenario==0, ['Variable','Value']]
global_values.Value = global_values.apply(lambda x: metric_fmt(x).format(x.Value), axis=1)
global_values.columns = ['Global Metric', 'Value']
global_values

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Line and Bar Chart

# COMMAND ----------

variables = ['Account Value','Cash Surrender Value', 'GMWB Benefit Base', 'GMDB Benefit Base']
dff = df.filter(df.Variable.isin(variables)).toPandas()

# COMMAND ----------

avg = dff['LoadId Variable ProjYear Value'.split()].groupby(by='LoadId Variable ProjYear'.split()).mean().reset_index()

# COMMAND ----------

px.line(avg, x='ProjYear', y='Value', color='Variable', facet_row='LoadId', title = 'Undiscounted Account Value')

# COMMAND ----------

rate = 0.04
avg['Present Value'] = avg.apply(lambda x: x.Value / (1+rate)**x.ProjYear, axis=1)
px.line(avg, x='ProjYear', y='Present Value', color='Variable', facet_row='LoadId', title='Account Value Discounted at 4%')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Waterfalls
# MAGIC Waterfall charts are commonly used to visualize cashflows from the Income Statement. The Plotly library has built a built in waterfall function for plotting. 

# COMMAND ----------

variables = [
    'Risk Fee Separate Account Total',
    'Separate Account Rider Fees',
    'Mutual Fund Service Fee Payments Total',
    'Separate Account Total Surrender Charges',
    'Contract Maintenance Charge',
    'Separate Account Policy Expenses',
    'Commission Net of Chargebacks',
    'Hedge Costs',
    'Gross Death Benefit',
    'General account partial withdrawal benefit',
    'GMWB Hedge Benefit Pricing Proxy',
    'Change In Net Actuarial Reserve',
    'Change In Required Surplus',
    'Investment Income on CF and Reserve and Surplus',
    'Income Tax',
]

dff = df.filter(df.Variable.isin(variables)).toPandas()
avg = dff['LoadId Variable ProjYear Value'.split()].groupby(by='LoadId Variable ProjYear'.split()).mean().reset_index()

# COMMAND ----------

flip = [
    'Commission Net of Chargebacks',
    'GMWB Hedge Benefit Pricing Proxy',
    'Change In Net Actuarial Reserve',
    'Income Tax',
]

vrs = []
for var in variables: 
  if var in avg.Variable.unique(): 
    vrs.append(var)

x = [   
  ['Revenue']*5 + ['Expenses']*3 + ['Benefits']*3 + ['Change In Int']*3 + ['Income Tax'] + ['DE'] 
  ,vrs + ['Distributable Earnings']
]

y = avg.groupby(by=['Variable'])['Value'].sum()
y = [y[vr]*-1 if vr in flip else y[vr] for vr in vrs]
y = [*y, sum(y)]

text =['${:,.2f}'.format(val) for val in y]

trace = go.Waterfall(
    orientation = "v",
    measure = ["relative" for x in x[0][:-1]] + ["total"],
    x=x,
    y=y,
    text=text,
    textposition = "outside",
)

fig = go.Figure([trace], go.Layout(height=1000))
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## More Advanced Analysis 
# MAGIC Let's look at correlations between ROTC, PVDE, ROA, etc... with the IDF Moderate Index. 
# MAGIC 
# MAGIC Below, I specify the variables that I will be looking at and filter the original dataframe to the columns and values for this analysis. 

# COMMAND ----------

main_metrics = ['Becker_PVDE', 'Becker_ROTC', 'Breakeven Month', 'Expected Policy Life', 'UndiscountedROA', 'IDF Moderate Index']
dff = df.filter(df.Variable.isin(main_metrics)).toPandas().sort_values(by=['Variable', 'Value'])['LoadId Scenario Variable ProjYear Value'.split()]

# COMMAND ----------

# MAGIC %md
# MAGIC For IDF Moderate Index, we will look at the average present value of the index for the the first 10 years. The lines below 
# MAGIC 1. filter the dataframe to the first 10 years
# MAGIC 2. calculate the present value of the index, and 
# MAGIC 3. pivot the data such that the variables are the column headers. 

# COMMAND ----------

dff = dff.loc[(dff.ProjYear <= 10) & (dff.Scenario != 0)]
dff.loc[dff.Variable == 'IDF Moderate Index', 'Value'] = dff.loc[dff.Variable == 'IDF Moderate Index', ].apply(lambda x: 1/(1+x.Value), axis=1)
dff_pvt = dff.pivot_table(index=['LoadId', 'Scenario'], values='Value', columns='Variable', aggfunc='mean')

# COMMAND ----------

# MAGIC %md
# MAGIC I am using a heatmap and a scatter plot matrix to analyze the correlation between the scenario dependent metric and the summary pricing metrics. The correlation matrix for the data is made using the ```corr``` method of the data table.

# COMMAND ----------

data = dff_pvt.corr()
data

# COMMAND ----------

# MAGIC %md
# MAGIC Heatmaps are made using the data from the correlation matrix. The Plotly library includes a heatmap plot, which is used in the cell below. 

# COMMAND ----------

go.Figure(
  go.Heatmap(
    z=data,
    x=dff_pvt.columns,
    y=dff_pvt.columns,
  ),
  go.Layout(
    height=600,
    width=700,
    title = 'Main Metrics Correlation Analysis with PV of IDF'
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Last, plotly express includes a scatter matrix plot. This generates a matrix of scatter plots showing the relationship between two variables (e.g. linear, exponentional, etc.)
# MAGIC 
# MAGIC The matrix below shows that the average present value of IDF moderate is linearly related to Becker PVDE and Becker ROTC. 

# COMMAND ----------

px.scatter_matrix(dff_pvt, height = 1200, width = 1200, title='Scatter Plot Matric of Major Metrics')

# COMMAND ----------

# MAGIC  %sql
# MAGIC   select
# MAGIC     LoadId,
# MAGIC     OuterScenarioId as Scenario,
# MAGIC     cvar.VariableName as Variable,
# MAGIC     rvar.VariableName as RawVariable,
# MAGIC     pcs.PostCalcEqu as Equation,
# MAGIC     ProjYear, 
# MAGIC     Cast(VariableValue as float) as Value
# MAGIC   from nfaprod_mo_conformed.modelvalue mval
# MAGIC   join nfaprod_mo_conformed.modelvariablexref mvx 
# MAGIC     on mval.ModelVariableId = mvx.SourceModelVariableId
# MAGIC     and mvx.EndDate >= current_date()
# MAGIC   join nfaprod_mo_conformed.modelvariable rvar 
# MAGIC     on mval.ModelVariableId = rvar.ModelVariableId
# MAGIC     and rvar.EndDate >= current_date()
# MAGIC   join nfaprod_mo_conformed.modelvariable cvar 
# MAGIC     on mvx.CommonModelVariableId = cvar.ModelVariableId
# MAGIC     and cvar.EndDate >= current_date()
# MAGIC   left join nfaprod_mo_conformed.postcalcs pcs 
# MAGIC     on rvar.VariableName = pcs.PostCalcVar
# MAGIC     and rvar.SourceSystemID = pcs.SourceSystemID
# MAGIC     and pcs.EndDate >= current_date()
# MAGIC   where mval.LoadId = 41559
