-- Databricks notebook source
-- DBTITLE 1,Create Widget in SQL
CREATE WIDGET TEXT LoadID_Eagle DEFAULT "";
CREATE WIDGET TEXT LoadID_VA DEFAULT "";
CREATE WIDGET TEXT LastYearIncl DEFAULT "";

-- COMMAND ----------

-- DBTITLE 1,Remove Widget in SQL
--REMOVE WIDGET  LoadID_VA ;

-- COMMAND ----------

-- DBTITLE 1,Use Widget in SQL query
SELECT COUNT(*) AS cnt
FROM NFAPROD_MO_VIEWS.VW_MODELVALUE_GET
WHERE ModelGroupValue not in ('.VA\S\NLIC-PII',
                                '.VA\S\NLIC-IPG',
                                'VA\F\EAGLE_PLUSNC.1',
                                'VA\F\FORECAST-STAT-EAGLE.1')
        AND ((LoadID = '$LoadID_VA') OR (LoadID = '$LoadID_Eagle'))
        AND YEAR(OuterScenarioDate) <= '$LastYearIncl'
        AND InnerScenarioID = 0
;

-- COMMAND ----------

-- DBTITLE 1,Above SQL W/O widgets
select COUNT(*) AS cnt
FROM NFAPROD_MO_VIEWS.VW_MODELVALUE_GET
WHERE ModelGroupValue not in ('.VA\S\NLIC-PII',
                                '.VA\S\NLIC-IPG',
                                'VA\F\EAGLE_PLUSNC.1',
                                'VA\F\FORECAST-STAT-EAGLE.1')
        AND ((LoadID = 80078) OR (LoadID = 80247))
        AND YEAR(OuterScenarioDate) <= 2025
        AND InnerScenarioID = 0
;

-- COMMAND ----------

-- DBTITLE 1,Declare Notebook Local Parameters
-- MAGIC %python
-- MAGIC pLoadID_Eagle = 80242;
-- MAGIC pLoadID_VA = 80071;
-- MAGIC pLastYearIncl = 2026;

-- COMMAND ----------

-- DBTITLE 1,Use Widget in SQL
SELECT COUNT(*) AS cnt
--,getArgument("cuts") AS cuts 
FROM NFADEV_MO_VIEWS.VW_MODELVALUE_GET
WHERE ModelGroupValue not in ('.VA\S\NLIC-PII',
                                '.VA\S\NLIC-IPG',
                                'VA\F\EAGLE_PLUSNC.1',
                                'VA\F\FORECAST-STAT-EAGLE.1')
        AND ((LoadID = '$pLoadID_VA') OR (LoadID = '$pLoadID_Eagle'))
        AND YEAR(OuterScenarioDate) <= '$pLastYearIncl'
        AND InnerScenarioID = 0
;
