-- Databricks notebook source
select VariableName, SourceSystemID, EndDate, ModelVariableId from nfaprod_mo_conformedwork.modelvariable;

-- COMMAND ----------

select GroupID, EndDate, VarID from nfaprod_mo_conformedwork.variablegroups;

-- COMMAND ----------

SELECT datediff('2009-07-31', '2009-07-30');
SELECT cast(months_between('2009-08-31', '2009-07-30')as int);
use nfadev_mo_conformedwork;
create table results (
	LoadId int,
	VariableName varchar(75),
    CommonName varchar(75),
    Equation varchar(1000), 
	OuterScenarioId int,
	Epoch int,
	VariableValue float
);

-- COMMAND ----------

SELECT quarter(ValuationDate) - quarter(InnerScenarioDate) from nfaprod_mo_conformed.modelvalue V;
--SELECT quarter(InnerScenarioDate) from nfaprod_mo_conformed.modelvalue V;

-- COMMAND ----------

Select RunID, InnerScenarioId,VariableName,ValuationDate,    0,   1,   2
From
    (
		select R.Runid,rie.ScenarioName,Mv.VariableName,v.InnerScenarioId,v.valuationdate,v.VariableValue,quarter(ValuationDate) - quarter(InnerScenarioDate) as ProjPeriod 
		from nfaprod_mo_conformed.modelvalue V
		left Join nfaprod_mo_conformed.modelvariable MV on mv.ModelVariableId = v.ModelVariableId
		inner join nfaprod_mo_conformed.runinfo R on R.LoadID = v.LoadId
		left join nfaprod_mo_axis.runinfoextra as rie on rie.RunId=r.RunId and rie.ScenarioId=v.InnerScenarioId
		where R.RunId = 660626845 
		and r.LoadID = 107191
		and VariableName = 'Present Value of Surplus'
	)
	Pivot (
			Sum(VariableValue)
			For ProjPeriod in (   0,   1,   2)
)

-- COMMAND ----------

show databases like 'nfadev*';
--use nfadev_mo_conformedwork; show tables;

-- COMMAND ----------

with startDate as (	
select 	
        LoadId 
        ,last_day(add_months(min(OuterScenarioDate),1))
    from nfaprod_mo_conformed.modelvalue 	
    where LoadId in (57017, 42414, 42212, 42283) 	
    group by LoadId	
)

-- COMMAND ----------

with startDate as (	
    select 	
        LoadId 	
        ,last_day(add_months(min(OuterScenarioDate),1)) as StartDate	
    from nfaprod_mo_conformed.modelvalue 	
    where LoadId in (78125)	
    group by LoadId	
)	
select	
    mval.LoadID,	
	SourceSystemID,
	ValuationDate, 
    VariableName,	
	avg(VariableValue) as AverageValue,
	sum(VariableValue) as SumValue
from nfaprod_mo_conformed.modelvalue mval	
join nfaprod_mo_conformedwork.modelvariable mvar	
--join modeloutput.Conformed.modelvariable mvar	
    on mval.ModelVariableId = mvar.ModelVariableId	
    and mvar.EndDate >= current_date()	
join startDate sd 	
    on mval.LoadId = sd.LoadId	
	where outerscenarioid != 0
	And OuterScenarioDate >= sd.StartDate
--where 	mval.OuterScenarioDate <= sd.StartDate
--    (	
--        mvar.VariableName in ('CALC_Becker_ROTC', 'CALC_Becker_PVDE', 'Calc_Breakeven Month', 'CALC_CTE95', 'CALC_Risk Metric', 'ROA', 'CALC_UndiscountedROA') 	
 --       and 	
 --       OuterScenarioId = 0	
--    )	
--    or 	
 --   (	
 --       mvar.VariableName in ('CALC_Capital', 'CALC_Expected Policy Life', 'CALC_Units Issued')	
--        and 	
 --       mval.OuterScenarioDate < sd.StartDate	
--    )	
 --   or 	
 --   (	
  --      mvar.VariableName in ('CashPrem', 'Annuity fund + deposits')	
  --      and 	
  --      mval.OuterScenarioDate = sd.StartDate	
 --   )	
group by VariableName, mval.LoadId, ValuationDate, SourceSystemId	
order by mval.LoadId, VariableName	



-- COMMAND ----------

SELECT  distinct (ari.projectiondescription)
                    FROM nfaprod_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfaprod_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfaprod_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfaprod_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfaprod_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Testing' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV'
                          and lower(projectiondescription) like ('%iul/eiul%')
                          
                     

-- COMMAND ----------

SELECT  distinct (ari.projectiondescription)
                    FROM nfaprod_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfaprod_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfaprod_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfaprod_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfaprod_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Testing' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV'
                          and lower(projectiondescription) like ('%iul/eiul%')
                          
                     

-- COMMAND ----------

SELECT  ari.projectiondescription, purpose, application, sourcevarname,
                       LC.notes
                       --,count(*)
                    FROM nfaprod_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfaprod_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfaprod_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfaprod_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfaprod_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Testing' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV'
                          and notes is not null
                          and lower(projectiondescription) like ('%iul/eiul%')
                          and (notes like ('%20Q2.03%') or notes like ('%20Q2.02%'))
--(, 'iat iul/eiul', 'iul/eiul iat classic self', 'iul/eiul iat classic lapse', 'iat production iul accum', 'iat iul accum', 'iat production iul prot', 'iat iul prot', 'iat production iul accum2', 'iat iul accum2'')

-- COMMAND ----------

-- DBTITLE 1,Model Testing Dashboard Testing
SELECT  ari.projectiondescription, purpose, application, sourcevarname,
                       LC.notes
                       ,count(*)
                    FROM nfaprod_mo_views.vw_modelvalue_get As mvl
                    INNER JOIN nfaprod_mo_conformed.RunInfo AS ri ON ri.LoadID=mvl.LoadId
                    INNER JOIN nfaprod_mo_ALFA.RunInfo AS ari ON ari.RunID=ri.RunId
                    INNER JOIN nfaprod_mo_conformed.loadclassifications as LC on LC.LoadID = mvl.loadid
                    INNER JOIN nfaprod_mo_conformed.loadapplications as LA on LA.classificationID = LC.classificationID
                    WHERE purpose = 'Testing' and
                          application = 'Illustration Actuary Test' and
                          mvl.sourcevarname = 'CALC_IAT_AccumAssetPlusNetCFMinusCV'
                          and notes is not null
--and lower(projectiondescription) in ('iat production iul/eiul', 'iat iul/eiul', 'iul/eiul iat classic self', 'iul/eiul iat classic lapse', 'iat production iul accum', 'iat iul accum', 'iat production iul prot', 'iat iul prot', 'iat production iul accum2', 'iat iul accum2', 'iat production iul prot2', 'iat iul prot2', 'iat production caul', 'iat caul', 'iat production sul1', 'iat sul1', 'iat production sul2', 'iat sul2', 'iat production spul', 'iat spul', 'iat production ultimate', 'iat ultimate', 'iat production nlg ul', 'iat nlg ul', 'iat production legacy ul and refix', 'iat legacy ul and refix', 'iat production legacy sul', 'iat legacy sul', 'iat production ul bel 80', 'iat ul bel 80', 'iat production ulp', 'iat ulp', 'iat production provident ul 100', 'iat provident ul 100', 'iat production provflex', 'iat provflex', 'iat production boli', 'iat boli', 'iat production feul', 'iat feul', 'iat production non par wl', 'iat non par wl', 'iat production 20 pay non par wl', 'iat 20 pay non par wl', 'iat production pro term', 'iat pro term', 'iat production equity plus & lpu 95', 'iat equity plus & lpu 95', 'iat production intersector wl', 'iat intersector wl', 'iat production wl ii', 'iat wl ii testing', 'iat production provident term (step1 and preference term)', 'iat provident term (step1 and preference term) testing', 'ul 1 iat', 'ul 3 iat', 'ul 4 iat (single prem)', 'ul le prem)', 'ul 4 iat (flex prem)', 'ul  prem)', 'ul 5 iat', 'ul 6 iat', 'ul 7 iat', 'iat production lfcg001', 'iat lfcg001', 'iat production lfcf001', 'iat lfcf001', 'iat production lfcg027', 'iat lfcg027')
group by ari.projectiondescription, purpose, application, sourcevarname, LC.notes

-- COMMAND ----------

select sourcevarname,LineNo_StatIncPctRes,count(*) from (
select 
                        t.runid,
                        t.sourcevarname,
                        t.variablevalue, 
                        t.outerscenariodate as Year,
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
                        end as LineNo_StatInc
                        ,case when lower(sourcevarname) = 'net premium - payout annuity' then 1 
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
                          when lower(sourcevarname) = 'stat res - net reserve' then 18
                          else 19
                        end as LineNo_StatIncPctRes
                        from 
                          nfaprod_mo_views.vw_modelvalue_get t
                        where runid = 655766312 and
                          t.outerscenarioId = 1                     
                          and lower(t.sourcevarname) in ('net premium - payout annuity', 'expenses - first year', 'expenses - renewal', 'net benefits - death', 'net benefits - annuity', 'total net cashflow', 'change in actuarial reserve', 'investment inc on cashflow & res', 'invest inc adjustment on cf & res', 'corporate tax on cashflow & res', 'profit on cashflow & reserve', 'change in required surplus', 'before tax income on req surplus', 'invest inc adjustment on req surp', 'total inv income on req surplus', 'corporate tax on required surplus', 'contribution to free surplus ctfs', 'stat res - net reserve', 'req surplus1 - required capital - c1', 'net investment income = i', 'income from realized g/l')
                      --  group by
                      --    t.runid,
                      --    t.sourcevarname,
                      --    t.outerscenariodate,
                      --    t.variablevalue 
                      )
                      where LineNo_StatIncPctRes = 19
                      group by sourcevarname ,LineNo_StatIncPctRes
-- count with group by 11900
-- 595 2380 17 statinc
-- 595 1785 18 statinc

-- COMMAND ----------

SELECT distinct purpose
FROM nfaprod_mo_conformed.loadclassifications

-- COMMAND ----------

SELECT distinct notes
FROM nfaprod_mo_conformed.loadclassifications

-- COMMAND ----------

select count(distinct LoadId, ProjectionDescription) from nfaprod_mo_alfa.runinfo;

-- COMMAND ----------

With MO123 as (
  SELECT
    InnerScenarioId,
    InnerScenarioDate
  from
    (
      SELECT
        CMV.ModelVariableId,
        MV.VariableValue,
        MV.InnerScenarioDate,
        MV.InnerScenarioId
      FROM
        nfaprod_mo_Conformed.ModelVariable CMV
        INNER JOIN nfaprod_mo_Conformed.ModelValue MV ON MV.ModelVariableId = CMV.ModelVariableId -----------------------------------------------------------------
      WHERE
        CMV.SourceSystemId = 2
        AND CMV.ModelVariableId IN (
          2172,
          2175,
          2215,
          2212,
          2218,
          1783,
          1809,
          1855,
          1768,
          1834,
          1826,
          1762
        )
        AND MV.LoadId in (
          -- Get the loadid of Block 1
          SELECT
            MAX(r.loadid)
          from
            nfaprod_mo_Conformed.RunInfo R
            INNER JOIN nfaprod_mo_Conformed.LoadClassifications L ON l.LoadId = r.LoadID
            INNER JOIN nfaprod_mo_Conformed.LoadApplications as la ON la.ClassificationID = l.ClassificationID
          GROUP BY
            Application
        )
    )AS BaseTall  PIVOT(SUM(VariableValue) FOR ModelVariableId in (2172, 2175, 2215, 2212, 2218, 1783, 1809, 1855, 1768, 1834, 1826, 1762))  'PivotTable'
)

-- COMMAND ----------

Select *
from nfaprod_mo_views.vw_modelvalue_get V
WHERE V.LoadID in ('83315', '83316', '83317','83318','81029','81408','81409','82558') or runid = 652525058

-- COMMAND ----------

-- MAGIC %sql select * from nfadev_mo_moatref.HestonVol_SVW_Policies

-- COMMAND ----------

-- DBTITLE 1,30 day rate
-- MAGIC %python
-- MAGIC HestonVol_SVW_Policies_df = spark.sql("""
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   (
-- MAGIC     select
-- MAGIC       Scenario,
-- MAGIC       Rate_30d,
-- MAGIC       row_number() OVER (
-- MAGIC         PARTITION BY Scenario
-- MAGIC         ORDER BY
-- MAGIC           Scenario ASC
-- MAGIC       ) as Month
-- MAGIC     from
-- MAGIC       nfadev_mo_moatref.HestonVol_SVW_Policies
-- MAGIC   ) PIVOT (sum(Rate_30d) FOR Scenario IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768,769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,900,901,902,903,904,905,906,907,908,909,910,911,912,913,914,915,916,917,918,919,920,921,922,923,924,925,926,927,928,929,930,931,932,933,934,935,936,937,938,939,940,941,942,943,944,945,946,947,948,949,950,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968,969,970,971,972,973,974,975,976,977,978,979,980,981,982,983,984,985,986,987,988,989,990,991,992,993,994,995,996,997,998,999,1000))
-- MAGIC   where Month < 498
-- MAGIC order by
-- MAGIC   month
-- MAGIC """)
-- MAGIC HestonVol_SVW_Policies_pdf = HestonVol_SVW_Policies_df.toPandas()
-- MAGIC HestonVol_SVW_Policies_pdf_T = HestonVol_SVW_Policies_pdf.drop(['Month'], axis=1).T
-- MAGIC display(HestonVol_SVW_Policies_pdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(HestonVol_SVW_Policies_pdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(HestonVol_SVW_Policies_pdf_T)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC BenefitsPivotProjid_pdf = pd.read_csv("/dbfs/FileStore/tables/BenefitsPivotProjid-1.csv")
-- MAGIC display(BenefitsPivotProjid_pdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC SurrProb_pdf = pd.read_csv("/dbfs/FileStore/tables/SurrProb.csv")
-- MAGIC display(SurrProb_pdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC days30_rate_pdf = pd.read_csv("/dbfs/FileStore/tables/30days_rate.csv")
-- MAGIC days30_rate_pdf_T = days30_rate_pdf.drop(['Months'], axis=1).T
-- MAGIC display(days30_rate_pdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(days30_rate_pdf_T)
-- MAGIC Non_Performance_Spread = 0.0012
-- MAGIC Illiquidity_Spread = 0.0030

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import numpy as np
-- MAGIC 
-- MAGIC days30_rate_series_outerid = days30_rate_pdf_T.iloc[15]
-- MAGIC #print(days30_rate_series_outerid)
-- MAGIC days30_rate_series_outerid_cpy = days30_rate_series_outerid
-- MAGIC days30_rate_series_outerid_lst = []
-- MAGIC counter = 1
-- MAGIC for i in range(0,days30_rate_series_outerid.count()):
-- MAGIC   if i > 0:
-- MAGIC     first =  days30_rate_series_outerid_cpy[i-1]
-- MAGIC     second_1 = days30_rate_series_outerid_cpy[i]
-- MAGIC     second = np.power((1 + second_1 + Non_Performance_Spread + Illiquidity_Spread), -1/12) 
-- MAGIC     temp = first * second
-- MAGIC     days30_rate_series_outerid_cpy[i] = temp
-- MAGIC     if i % 3 == 0:
-- MAGIC       print(temp," vale", i, "-", counter)
-- MAGIC       counter = counter + 1
-- MAGIC       days30_rate_series_outerid_lst.append(temp)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(days30_rate_series_outerid_lst)
-- MAGIC print(type(days30_rate_series_outerid_lst[1]))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC BenefitsPivotProjid_series_outerid = BenefitsPivotProjid_pdf.iloc[5,2:]
-- MAGIC print(BenefitsPivotProjid_series_outerid)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC SurrProb_series_outerid = SurrProb_pdf.iloc[5,2:]
-- MAGIC print(SurrProb_series_outerid)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in range(0,100):
-- MAGIC   print(BenefitsPivotProjid_series_outerid[i])
-- MAGIC   print(SurrProb_series_outerid[i])
-- MAGIC   print(days30_rate_series_outerid_lst[i])
-- MAGIC   val = float(BenefitsPivotProjid_series_outerid[i]) * SurrProb_series_outerid[i] * days30_rate_series_outerid_lst[i]
-- MAGIC   print(val, " - ", i, "\n-----------")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(len(days30_rate_series_outerid_lst))

-- COMMAND ----------

select * from nfadev_ls_correspondtest.modeltestinggeneralvalues limit 10

-- COMMAND ----------

select * from nfadev_ls_correspondtest.defmodeltestingamountcodes limit 10

-- COMMAND ----------

select * from nfadev_ls_correspondtest.modeltesting_driver limit 10

-- COMMAND ----------

select * from nfaprod_mo_conformed.runinfo

-- COMMAND ----------

select
  week(DBSRunTime)
  ,count(distinct RunId)
from
  nfaprod_mo_conformed.runinfo
group by
  LoadId 

-- COMMAND ----------

select count(distinct DBSRunID) from nfaprod_mo_conformed.runinfo

-- COMMAND ----------

show create table nfadev_mo_rptinput.arborist_loadentry;

-- COMMAND ----------

select * from nfadev_mo_rptinput.arborist_loadentry
