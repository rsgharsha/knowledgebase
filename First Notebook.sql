-- Databricks notebook source
show databases;

-- COMMAND ----------

select * from nfaprod_mo_conformed.modelvalue limit 10;
show databases;

-- COMMAND ----------

select count(*) from nfaprod_mo_conformed.modelvalue ;

-- COMMAND ----------

select * from nfaprod_mo_views.vw_modelvalue_get;

-- COMMAND ----------

desc nfaprod_mo_views.vw_modelvalue_get;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("show create table nfaprod_mo_views.vw_modelvalue_get").show(20,False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC pd.set_option('display.max_colwidth', 100)

-- COMMAND ----------

show create table nfaprod_mo_views.vw_modelvalue_get;

-- COMMAND ----------

select count(*) from (
select count (*), 
RunId,Loadid,ValuationDate,Modelvariableid,ModelGroupType,Modelgroupvalue,SourceVarName,CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate,Innerscenariodate,VariableValue,PostCalcEqu
from nfaprod_mo_views.vw_modelvalue_get
where runid in (650813575,650817207,650821472,650824722)
group by RunId,Loadid,ValuationDate,Modelvariableid,ModelGroupType,Modelgroupvalue,SourceVarName,CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate,Innerscenariodate,VariableValue,PostCalcEqu
)

-- COMMAND ----------

select count (distinct 
RunId,Loadid,ValuationDate,Modelvariableid,ModelGroupType,Modelgroupvalue,SourceVarName,CommonVarName, SrcSystemName, Outerscenarioid, Outerscenariodate,Innerscenariodate,VariableValue,PostCalcEqu)
from nfaprod_mo_views.vw_modelvalue_get
where runid in (650813575,650817207,650821472,650824722)

-- COMMAND ----------

select count (distinct 
runid,
                sourcevarname,
                variablevalue,
                OuterScenarioId,
                outerscenariodate)
from nfaprod_mo_views.vw_modelvalue_get
where runid in (650813575,650817207,650821472,650824722)

-- COMMAND ----------

select count(*) from nfaprod_mo_views.vw_modelvalue_get where runid in (650813575,650817207,650821472,650824722);

-- COMMAND ----------

use nfaprod_mo_alfa; show tables;

-- COMMAND ----------

show create table nfaprod_mo_conformed.modelvalue;

-- COMMAND ----------

describe detail nfaprod_mo_alfa.appcatalog;

-- COMMAND ----------

describe formatted nfaprod_mo_views.vw_modelvalue_get;

-- COMMAND ----------

--describe formatted nfadev_mo_conformed.LoadClassifications;
describe formatted nfadev_mo_conformed.modelvalue;

-- COMMAND ----------

select * from `nfadev_mo_moatref`.`PricingTableau_GetScenario`;

-- COMMAND ----------

select count(*) from nfadev_mo_conformed.runinfo where LoadId = '78122';


-- COMMAND ----------

select * from nfadev_mo_conformed.runinfo where LoadDate like '2020-07-13%';

-- COMMAND ----------

select min(LoadDate), max(LoadDate) from nfadev_mo_conformed.runinfo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_runinfo = spark.read.table('nfadev_mo_conformed.runinfo')
-- MAGIC df_runinfo.createOrReplaceTempView('runinfo')
-- MAGIC runinfo.show(10)
-- MAGIC 
-- MAGIC 
-- MAGIC df_LoadClass = spark.read.table('nfadev_mo_conformed.LoadClassifications')
-- MAGIC df_LoadClass.createOrReplaceTempView('LoadClass')
-- MAGIC 
-- MAGIC df_mval = spark.sql('select * from nfadev_mo_conformed.modelvalue')
-- MAGIC 
-- MAGIC 
-- MAGIC df_LoadApp = spark.read.table('nfadev_mo_conformed.LoadApplications')
-- MAGIC df_LoadApp.createOrReplaceTempView('LoadApp')
-- MAGIC 
-- MAGIC df_LoadId = df_runinfo.join(df_LoadClass, 'LoadId', 'left').join(df_LoadApp, df_LoadApp['ClassificationId']==df_LoadClass['ClassificationId'], 'left').filter(df_LoadApp['Application'].like('%Pricing%')).select('LoadId').distinct()
-- MAGIC 
-- MAGIC #df_LoadId.show(10000)

-- COMMAND ----------

select count(*) from nfaprod_mo_conformed.runinfo where runid = '78122';

-- COMMAND ----------

--select count(*) from nfadev_mo_conformed.LoadClassifications where LoadId = '78122';
--select * from nfaprod_mo_conformed.LoadClassifications where LoadId = '78122';
insert into table nfadev_mo_conformed.LoadClassifications Values ('143128','62c88265-2219-4693-bcc3-9aebbfa33cde','2020-07-14T08:29:50.532+0000',24036,78122,'NA','7/15/2020','2020-06-15T14:50:08.340+0000','peakek1');
select count(*) from nfadev_mo_conformed.LoadClassifications where LoadId = '78122';

-- COMMAND ----------

select count(*) from nfadev_mo_conformed.LoadApplications where loadid = 78122;

-- COMMAND ----------

select * from nfadev_mo_conformed.modelvalue;

-- COMMAND ----------

desc nfadev_mo_beowulf.RunInfo;

-- COMMAND ----------

select * from nfadev_mo_conformed.runinfo where loadid = 78122;

-- COMMAND ----------

select count(*) from nfadev_mo_conformed.sourcesystem;

-- COMMAND ----------

select count(*) from nfadev_mo_conformed.LoadClassifications where LoadId = '78122';

-- COMMAND ----------

select count(*) from `nfadev_mo_moatref`.`PricingTableau_GetScenario`;
select * from `nfadev_mo_moatref`.`PricingTableau_GetScenario` limit 10;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import * 
-- MAGIC 
-- MAGIC print(current_timestamp())
-- MAGIC print(from_utc_timestamp(current_timestamp(), 'EST5EDT')) 

-- COMMAND ----------

show databases like "nfadev_mo_ggyaxisraw";

-- COMMAND ----------

use nfadev_mo_ggyaxisraw;
show tables;

-- COMMAND ----------

select * from nfadev_mo_beowulf.RunInfo where JobName = 'EIA_Liability_Valuation' --and FileMD5Hash = '08215cc39807ad19c4027479c9fe1391'

-- COMMAND ----------

select * from nfaprod_mo_audit.etlpipelinestatus;

-- COMMAND ----------

set spark.sql.legacy.parquet.datetimeRebaseModeInRead = LEGACY;
with stg as (
              select V.LoadId, V.ModelVariableId VariableID, V.ModelGroupValue, V.SourceVarName VarName, V.CommonVarName, V.SrcSystemName,                   V.OuterScenarioId ScenarioID, V.OuterScenarioDate Period,V.OuterProjMo, V.InnerScenarioId,	V.InnerScenarioDate, V.InnerProjMo,	                 V.VariableValue Value 
              from nfaprod_mo_views.vw_modelvalue_get V 
              WHERE V.LoadID in (83315, 83316, 83317,83318,81029,81408,81409,82558)
     ),
     stg_f as (
                select * 
                from stg
                where VarName = "BeckPVDE" and Period = (
                                                          select min(Period)
                                                          from stg
                                                         )
                
     )
            
--select count(*) from stg_f ;
--where stg_f.CommonVarName is null

select count(*) from stg_f where stg_f.CommonVarName is not null;
