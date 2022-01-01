# Databricks notebook source
# MAGIC %sql
# MAGIC CACHE TABLE nfaprod_mo_conformed.modelvalue

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE AS SELECT * FROM nfaprod_mo_conformed.modelvalue WHERE ValuationDate > '2020-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86782,86780,86781,86204,86203,86202,86201) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86782,86780,86781,86204,86203,86202,86201) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86803, 86806, 86788, 86817, 86786, 86784, 86866, 86778, 86816, 86828) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (75761, 54882, 59352, 43471, 58498, 55701, 81128, 80970, 58502, 69950, 56278, 59178) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct LoadId from nfaprod_mo_conformed.modelvalue where ValuationDate > '2020-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select LoadId from nfaprod_mo_audit.modelvaluemetadata where ValuationDate > '2020-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC cache table as select * from nfadev_mo_conformed.modelvalue where LoadId in (78245,78235,78232,78122,78116,78111)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(VariableValue), LoadId FROM nfaprod_mo_conformed.modelvalue WHERE LoadId in (75505, 75504, 75502, 75500,75498,59068, 58564, 57360, 57359,57358, 57357, 57271, 57270 ) group by LoadId

# COMMAND ----------

# MAGIC %sql
# MAGIC cache select * from nfaprod_mo_conformed.modelvalue where LoadId in (34516,34513,34511,34453,34190,32153,29430,28726,28098,27563)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFadev_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (78235,78232,75505,75504,75502,75500,75498,59068,57360,57359) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86068,86067,86065,85827,86024,86072,86066,86071,86069,86205,88173) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC cache select * from nfaprod_mo_conformed.modelvalue where LoadId in (86068,86067,86065,85827,86024,86072,86066,86071,86069,86205,88173);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86068,86067,86065,85827,86024,86072,86066,86071,86069,86205,88173) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_lower__290714814__7_`, MAX(CAST(TRUNC(`custom_sql_query`.`Outerscenariodate`,'MM') AS DATE)) AS `temp_tmn_outerscenariodate_qk_upper__290714814__7_` FROM ( Select RunId, Loadid, ValuationDate, Modelvariableid, ModelGroupType, Modelgroupvalue, SourceVarName, CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate, OuterProjMo, Innerscenarioid, Innerscenariodate, InnerProjMo, VariableValue, SourceModelVariableId, CommonModelVariableId, MvarModelVariableId, VarComModelVariableId, PostCalcEqu, CommonVarsOnly from NFaprod_MO_VIEWS.VW_MODELVALUE_GET where LoadId in (86068,86067,86065,85827,86024,86072,86066,86071,86069,86205,88173) and CommonVarsOnly in (1,0) ) `custom_sql_query` GROUP BY 1.1000000000000001

# COMMAND ----------

#dbutils.fs.ls("dbfs:/cluster-logs/1029-142821-pekoe4/eventlog/1029-142821-pekoe4_10_142_16_196/6445151177414092103/eventlog")
dbutils.fs.ls("dbfs:/cluster-logs/1029-142821-pekoe4/eventlog/1029-142821-pekoe4_10_142_16_16/7289889914166662297/eventlog")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC dbfs ls /

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc -X GET -H 'Authorization: Bearer dapic919ead3b565905a4c0f270d4fcc7a06' \
# MAGIC "https://nationwide-prd.cloud.databricks.com/api/2.0/dbfs/list" \
# MAGIC --data '{ "path": "/cluster-logs/1029-142821-pekoe4/eventlog/1029-142821-pekoe4_10_142_16_196/6445151177414092103/eventlog" }' 
# MAGIC #| jq .

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc -X GET -H 'Authorization: Bearer dapic919ead3b565905a4c0f270d4fcc7a06' -H "Content-Type: application/json" \
# MAGIC "https://nationwide-prd.cloud.databricks.com/api/2.0/dbfs/read" \
# MAGIC --data '{ "path": "/cluster-logs/1029-142821-pekoe4/eventlog/1029-142821-pekoe4_10_142_16_196/6445151177414092103/eventlog" }' 
# MAGIC #| jq .

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "hi"
# MAGIC echo $DB_IS_DRIVER
# MAGIC 
# MAGIC lst_root_all=/*
# MAGIC lst_root_home_all=/home/*
# MAGIC lst_home_ubuntu_all=/home/ubuntu/*
# MAGIC 
# MAGIC echo "root all:-" $lst_root_all
# MAGIC echo "root home all:-" $lst_root_home_all
# MAGIC echo "root home  ubuntu all:-" $lst_home_ubuntu_all
# MAGIC 
# MAGIC lst_root_usr_all=/use/*
# MAGIC echo "root usr all:-" $lst_root_usr_all
# MAGIC 
# MAGIC pwd_root=/
# MAGIC pwd_home=~
# MAGIC echo "root path:-" $pwd_root
# MAGIC echo "home path:-" $pwd_home

# COMMAND ----------

dbutils.notebook.run("Cache table (sesha)", 60)
