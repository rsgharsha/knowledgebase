-- Databricks notebook source
-- DBTITLE 1,Scope
-- MAGIC %md This template includes reading data from Nationwide data lake, input user defined values using Widgets, staging temporary tables, calculating metrics, transformations, joins, pivoting and finally visualizing end results, using Spark SQL.
-- MAGIC Note:sql cannot have Variables

-- COMMAND ----------

-- DBTITLE 1,Widgets
CREATE WIDGET TEXT LoadID DEFAULT "";

-- COMMAND ----------

-- MAGIC %md [for detailed widgets notebook with samples: Right click here to open it in new tab](https://nationwide-prd.cloud.databricks.com/#notebook/756749/command/756750)

-- COMMAND ----------

SELECT COUNT(*) AS cnt
FROM NFAPROD_MO_VIEWS.VW_MODELVALUE_GET
join nfadev_mo_ALFA.RunInfo on 
WHERE ModelGroupValue not in ('.VA\S\NLIC-PII',
                                '.VA\S\NLIC-IPG',
                                'VA\F\EAGLE_PLUSNC.1',
                                'VA\F\FORECAST-STAT-EAGLE.1')
        AND ((LoadID = '$LoadID_VA') OR (LoadID = '$LoadID_Eagle'))
        AND YEAR(OuterScenarioDate) <= '$LastYearIncl'
        AND InnerScenarioID = 0
;

-- COMMAND ----------

-- DBTITLE 1,Aggregation
-- MAGIC %md [For Aggregation functions details: Right click here to open it in new tab](https://nationwide-prd.cloud.databricks.com/#notebook/1026271)

-- COMMAND ----------

-- DBTITLE 1,Spark SQL Joins
-- MAGIC %md
-- MAGIC Spark SQL Join is used to join two or more tables, It supports all basic join operations available in traditional SQL.
-- MAGIC 
-- MAGIC **join** operation takes parameters as below and returns DataFrame.
-- MAGIC 
-- MAGIC param other: Right side of the join
-- MAGIC param on: a string for the join column name
-- MAGIC param how: default inner. Must be one of 
-- MAGIC **inner join, cross join, outer join,full outer join, left join, left outer join, right join, right outer join,left semi join, and left anti join**
-- MAGIC 
-- MAGIC Inner Join - Inner join is the default join in PySpark and it’s mostly used. This joins two datasets on key columns, where keys don’t match the rows get dropped from both datasets
-- MAGIC 
-- MAGIC 
-- MAGIC Outer Join - Outer a.k.a full, fullouter join returns all rows from both datasets, where join expression doesn’t match it returns null on respective record columns.
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Left Join - Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.
-- MAGIC 
-- MAGIC 
-- MAGIC Right Join - Right a.k.a Rightouter join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset, when join expression doesn’t match, it assigns null for that record and drops records from left where match not found.
-- MAGIC 
-- MAGIC 
-- MAGIC Left Semi Join - 
-- MAGIC leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets.
-- MAGIC 
-- MAGIC 
-- MAGIC Left Anti Join - leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.

-- COMMAND ----------

-- DBTITLE 1,Spark SQL Queries 
-- MAGIC %md
-- MAGIC Spark SQL statements Queries are used to retrieve result sets from one or more tables. The following section describes the overall query syntax and the sub-sections cover different constructs of a query along with examples.
-- MAGIC 
-- MAGIC     SELECT [ hints - BROADCAST(tblName1), BROADCAST(tblName2) ] 
-- MAGIC     [ * | DISTINCT ]
-- MAGIC     
-- MAGIC     [aggregation - AVG, MIN, MAX, MEAN, COUNT, .... ]
-- MAGIC     
-- MAGIC     [CASE WHEN {expression} THEN A ELSE B END]
-- MAGIC     
-- MAGIC     FROM { from_item [ , ... ] }
-- MAGIC     
-- MAGIC     [ PIVOT clause ]
-- MAGIC     
-- MAGIC     [ LATERAL VIEW clause ] [ ... ]
-- MAGIC     
-- MAGIC     [ WHERE boolean_expression ]
-- MAGIC     
-- MAGIC     [ GROUP BY expression [ , ... ] ]
-- MAGIC     
-- MAGIC     [ HAVING boolean_expression ]
-- MAGIC     
-- MAGIC     
-- MAGIC **with_query**
-- MAGIC Specifies the common table expressions (CTEs) before the main query block. These table expressions are allowed to be referenced later in the FROM clause. This is useful to abstract out repeated subquery blocks in the FROM clause and improves readability of the query.
-- MAGIC 
-- MAGIC Syntax -      
-- MAGIC 
-- MAGIC       WITH q1 AS (SELECT key from src where key = '5') 
-- MAGIC       SELECT * from q1
-- MAGIC 
-- MAGIC **hints**
-- MAGIC Hints can be specified to help spark optimizer make better planning decisions. Currently spark supports hints that influence selection of join strategies and repartitioning of the data. 
-- MAGIC Syntax - /*+ BROADCAST(runinfo),BROADCAST(MVAR),BROADCAST(SS),BROADCAST(Xref),Broadcast(varcom), Broadcast(PC), Broadcast(H) */
-- MAGIC 
-- MAGIC **ALL**
-- MAGIC Select all matching rows from the relation and is enabled by default.
-- MAGIC 
-- MAGIC **DISTINCT**
-- MAGIC Select all matching rows from the relation after removing duplicates in results.
-- MAGIC 
-- MAGIC **named_expression**
-- MAGIC An expression with an assigned name. In general, it denotes a column expression.
-- MAGIC Syntax: expression [AS] [alias]
-- MAGIC 
-- MAGIC **CASE WHEN**
-- MAGIC CASE clause uses a rule to return a specific result based on the specified condition, similar to if/else statements in other programming languages.
-- MAGIC Syntax:  
-- MAGIC 
-- MAGIC     CASE [ expression ] { WHEN boolean_expression THEN then_expression } [ ... ]
-- MAGIC     [ ELSE else_expression ]
-- MAGIC     END
-- MAGIC 
-- MAGIC Example:
-- MAGIC     
-- MAGIC     SELECT id, CASE id WHEN 100 then 'bigger' WHEN  id > 300 THEN '300' ELSE 'small' END FROM person;
-- MAGIC 
-- MAGIC **from_item**
-- MAGIC Specifies a source of input for the query.
-- MAGIC Syntax: FROM
-- MAGIC 
-- MAGIC **Table relation**
-- MAGIC In Delta Lake, you specify a relation by specifying: table_name. In addition, you can specify a time travel version using TIMESTAMP AS OF, VERSION AS OF, or @ syntax, after your table identifier.
-- MAGIC 
-- MAGIC **TIME TRAVEL**
-- MAGIC Delta Lake time travel allows you to query an older snapshot of a Delta table.
-- MAGIC Example: 
-- MAGIC 
-- MAGIC     SELECT * FROM events TIMESTAMP AS OF timestamp_expression
-- MAGIC     SELECT * FROM events VERSION AS OF version
-- MAGIC   
-- MAGIC **PIVOT**
-- MAGIC The PIVOT clause is used for data perspective; We can get the aggregated values based on specific column value.
-- MAGIC 
-- MAGIC **LATERAL VIEW**
-- MAGIC The LATERAL VIEW clause is used in conjunction with generator functions such as EXPLODE, which will generate a virtual table containing one or more rows. LATERAL VIEW will apply the rows to each original output row.
-- MAGIC 
-- MAGIC **WHERE**
-- MAGIC Filters the result of the FROM clause based on the supplied predicates.
-- MAGIC 
-- MAGIC **GROUP BY**
-- MAGIC Specifies the expressions that are used to group the rows. This is used in conjunction with aggregate functions (MIN, MAX, COUNT, SUM, AVG, etc.) to group rows based on the grouping expressions and aggregate values in each group. When a FILTER clause is attached to an aggregate function, only the matching rows are passed to that function.
-- MAGIC 
-- MAGIC **HAVING**
-- MAGIC Specifies the predicates by which the rows produced by GROUP BY are filtered. The HAVING clause is used to filter rows after the grouping is performed. If HAVING is specified without GROUP BY, it indicates a GROUP BY without grouping expressions (global aggregate).
-- MAGIC 
-- MAGIC **ORDER BY**
-- MAGIC Specifies an ordering of the rows of the complete result set of the query. The output rows are ordered across the partitions. This parameter is mutually exclusive with SORT BY, CLUSTER BY and DISTRIBUTE BY and can not be specified together.
-- MAGIC 
-- MAGIC **SORT BY**
-- MAGIC Specifies an ordering by which the rows are ordered within each partition. This parameter is mutually exclusive with ORDER BY and CLUSTER BY and can not be specified together.
-- MAGIC 
-- MAGIC **CLUSTER BY**
-- MAGIC Specifies a set of expressions that is used to repartition and sort the rows. Using this clause has the same effect of using DISTRIBUTE BY and SORT BY together.
-- MAGIC 
-- MAGIC **DISTRIBUTE BY**
-- MAGIC Specifies a set of expressions by which the result rows are repartitioned. This parameter is mutually exclusive with ORDER BY and CLUSTER BY and can not be specified together.
-- MAGIC 
-- MAGIC **LIMIT**
-- MAGIC Specifies the maximum number of rows that can be returned by a statement or subquery. This clause is mostly used in the conjunction with ORDER BY to produce a deterministic result.
-- MAGIC 
-- MAGIC **boolean_expression**
-- MAGIC Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).
-- MAGIC 
-- MAGIC **expression**
-- MAGIC Specifies a combination of one or more values, operators, and SQL functions that evaluates to a value.
-- MAGIC 
-- MAGIC **named_window**
-- MAGIC Specifies aliases for one or more source window specifications. The source window specifications can be referenced in the widow definitions in the query.

-- COMMAND ----------

Select
* from (
SELECT /*+ BROADCAST(runinfo),BROADCAST(MVAR),BROADCAST(SS),BROADCAST(Xref),Broadcast(varcom), Broadcast(PC), Broadcast(H) */  --HINTS 
runinfo.RunId,
mval.Loadid,
mval.ValuationDate,
mval.Modelvariableid,
mval.ModelGroupType,
mval.Modelgroupvalue,
MVAR.Variablename AS SourceVarName,
varcom.VariableName AS CommonVarName,
ss.SrcSystemName,
mval.Outerscenarioid,
mval.Outerscenariodate,
mval.OuterScenarioProjMonth AS OuterProjMo,
 mval.Innerscenarioid,
mval.Innerscenariodate,
mval.InnerScenarioProjMonth AS InnerProjMo,
mval.VariableValue,
Xref.SourceModelVariableId,
Xref.CommonModelVariableId,
MVAR.ModelVariableId as mvarModelVariableId,
varcom.ModelVariableId as varcomModelVariableId,
PC.PostCalcEqu,
H.Report,
H.Tier1,
H.Tier2,
H.Tier3,
H.Tier4,
H.Tier5,
H.Model,
H.SignChangeFlag,
CASE WHEN Xref.SourceModelVariableId = mval.ModelVariableId and varcom.ModelVariableId = Xref.CommonModelVariableId THEN 1 ELSE 0 END as CommonVarsOnly
FROM nfaprod_mo_conformed.Modelvalue AS mval
INNER JOIN nfaprod_mo_conformed.RunInfo AS runinfo  ON runinfo.LoadId = mval.LoadId
INNER JOIN nfaprod_mo_conformed.ModelVariable AS MVAR  ON mval.ModelVariableId = mvar.ModelVariableId
and current_timestamp() between mvar.StartDate and mvar.EndDate
INNER JOIN nfaprod_mo_conformed.SourceSystem SS  ON ss.SourceSystemID = mvar.SourceSystemID
LEFT JOIN nfaprod_mo_conformed.Modelvariablexref AS Xref  ON xref.SourceModelVariableId = mval.ModelVariableId
and current_timestamp()  between Xref.StartDate and Xref.EndDate
LEFT JOIN nfaprod_mo_conformed.ModelVariable AS varcom  on varcom.ModelVariableId = Xref.CommonModelVariableId
and current_timestamp() between varcom.StartDate and varcom.EndDate
LEFT JOIN nfaprod_mo_conformed.PostCalcs AS PC  on PC.PostCalcVar = MVAR.VariableName
and current_timestamp() between PC.StartDate and PC.EndDate and PC.SourceSystemID = ss.SourceSystemID
LEFT JOIN nfaprod_mo_moatref.HierarchyRules H on lower(H.VariableName) = lower(MVAR.VariableName) and H.Model = ss.SrcSystemName
)
WHERE LOADID = $LoadID

-- COMMAND ----------

-- DBTITLE 1,Describe statement
-- MAGIC %md
-- MAGIC **DESCRIBE DATABASE**  - 
-- MAGIC DESCRIBE DATABASE statement returns the metadata of an existing database. The metadata information includes database name, database comment, and database location on the filesystem. If the optional EXTENDED option is specified, it returns the basic metadata information along with the database properties.
-- MAGIC 
-- MAGIC Syntax :   
-- MAGIC     { DESC | DESCRIBE } DATABASE [ EXTENDED ] db_name
-- MAGIC 
-- MAGIC **DESCRIBE TABLE**  - 
-- MAGIC DESCRIBE TABLE statement returns the basic metadata information of a table. The metadata information includes column name, column type and column comment.
-- MAGIC DESCRIBE TABLE EXTENDED - Display detailed information about the specified columns, including the column statistics collected.
-- MAGIC Syntax :  { DESC | DESCRIBE } TABLE [EXTENDED] [ format ] table_identifier [ partition_spec ] [ col_name ]
-- MAGIC 
-- MAGIC **DESCRIBE QUERY**  - 
-- MAGIC The DESCRIBE QUERY statement is used to return the metadata of output of a query.
-- MAGIC Syntax :
-- MAGIC       { DESC | DESCRIBE } [ QUERY ] input_statement

-- COMMAND ----------

-- DBTITLE 1,Spark SQL - Functions
-- MAGIC %md
-- MAGIC **like**  - 
-- MAGIC str like pattern - Returns true if str matches pattern, null if any arguments are null, false otherwise.
-- MAGIC 
-- MAGIC Example :
-- MAGIC SELECT '%SystemDrive%\Users\John' like '%Users%'
-- MAGIC 
-- MAGIC **ltrim**  - 
-- MAGIC ltrim(str)          - Removes the leading space characters from str.
-- MAGIC 
-- MAGIC ltrim(trimStr, str) - Removes the leading string contains the characters from the trim string
-- MAGIC 
-- MAGIC Arguments:
-- MAGIC 1. str - a string expression
-- MAGIC 2. trimStr - the trim string characters to trim, the default value is a single space
-- MAGIC 
-- MAGIC Example:  
-- MAGIC SELECT ltrim('    SparkSQL   ');
-- MAGIC 
-- MAGIC SELECT ltrim('Sp', 'SSparkSQLS');
-- MAGIC 
-- MAGIC **rtrim** - 
-- MAGIC rtrim(str) - Removes the trailing space characters from str.
-- MAGIC 
-- MAGIC rtrim(trimStr, str) - Removes the trailing string which contains the characters from the trim string from the str
-- MAGIC 
-- MAGIC Arguments:
-- MAGIC 1. str - a string expression
-- MAGIC 2. trimStr - the trim string characters to trim, the default value is a single space
-- MAGIC 
-- MAGIC Example:  SELECT rtrim('LQSa', 'SSparkSQLS');
-- MAGIC 
-- MAGIC **trim**
-- MAGIC trim(str) - Removes the leading and trailing space characters from str.
-- MAGIC 
-- MAGIC trim(BOTH trimStr FROM str) - Remove the leading and trailing trimStr characters from str
-- MAGIC 
-- MAGIC trim(LEADING trimStr FROM str) - Remove the leading trimStr characters from str
-- MAGIC 
-- MAGIC trim(TRAILING trimStr FROM str) - Remove the trailing trimStr characters from str
-- MAGIC 
-- MAGIC Arguments:
-- MAGIC 1. str - a string expression
-- MAGIC 2. trimStr - the trim string characters to trim, the default value is a single space
-- MAGIC 3. BOTH, FROM - these are keywords to specify trimming string characters from both ends of the string
-- MAGIC 4. LEADING, FROM - these are keywords to specify trimming string characters from the left end of the string
-- MAGIC 5. TRAILING, FROM - these are keywords to specify trimming string characters from the right end of the string
-- MAGIC 
-- MAGIC Examples: 
-- MAGIC SELECT trim('    SparkSQL   ');
-- MAGIC 
-- MAGIC SELECT trim(BOTH 'SL' FROM 'SSparkSQLS');
-- MAGIC 
-- MAGIC SELECT trim(LEADING 'SL' FROM 'SSparkSQLS');
-- MAGIC 
-- MAGIC **concat** - 
-- MAGIC concat(col1, col2, …, colN) - Returns the concatenation of col1, col2, …, colN.
-- MAGIC Example:  SELECT concat('Spark', 'SQL');
-- MAGIC 
-- MAGIC 
-- MAGIC **to_timestamp**  - 
-- MAGIC to_timestamp(timestamp[, fmt]) - Parses the timestamp expression with the fmt expression to a timestamp.
-- MAGIC 
-- MAGIC Example :  
-- MAGIC SELECT to_timestamp('2016-12-31 00:12:00');
-- MAGIC 
-- MAGIC SELECT to_timestamp('2016-12-31', 'yyyy-MM-dd');
-- MAGIC 
-- MAGIC **to_date**  - 
-- MAGIC to_date(date_str[, fmt]) - Parses the date_str expression with the fmt expression to a date. Returns null with invalid input. By default, it follows casting rules to a date if the fmt is omitted.
-- MAGIC 
-- MAGIC Example : 
-- MAGIC SELECT to_date('2009-07-30');
-- MAGIC 
-- MAGIC SELECT to_date('2016-12-31', 'yyyy-MM-dd');
-- MAGIC 
-- MAGIC **split**  - 
-- MAGIC split(str, regex) - Splits str around occurrences that match regex.
-- MAGIC 
-- MAGIC Example:  
-- MAGIC SELECT split('oneAtwoBthreeC', '[ABC]');
-- MAGIC 
-- MAGIC **regexp_replace**  - 
-- MAGIC regexp_replace(str, regexp, rep) - Replaces all substrings of str that match regexp with rep.
-- MAGIC 
-- MAGIC Example :  SELECT regexp_replace('100-200', '(\\d+)', 'num');
-- MAGIC 
-- MAGIC **regexp_extract**
-- MAGIC regexp_extract(str, regexp[, idx]) - Extracts a group that matches regexp.
-- MAGIC 
-- MAGIC Example : 
-- MAGIC SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
-- MAGIC 
-- MAGIC **substring** - 
-- MAGIC substring(str, pos[, len]) - Returns the substring of str that starts at pos and is of length len, or the slice of byte array that starts at pos and is of length len.
-- MAGIC 
-- MAGIC Example: 
-- MAGIC SELECT substring('Spark SQL', 5);
-- MAGIC 
-- MAGIC SELECT substring('Spark SQL', 5, 1);
-- MAGIC 
-- MAGIC **months_between**  - 
-- MAGIC months_between(timestamp1, timestamp2[, roundOff]) - Calculates the months between two dates. If timestamp1 is later than timestamp2, then the result is positive. If timestamp1 and timestamp2 are on the same day of month, or both are the last day of month, time of day will be ignored.
-- MAGIC 
-- MAGIC Example: 
-- MAGIC SELECT months_between('1997-02-28 10:30:00', '1996-10-30');
-- MAGIC 
-- MAGIC 
-- MAGIC **weekday**  - 
-- MAGIC weekday(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, …, 6 = Sunday).
-- MAGIC 
-- MAGIC Example : 
-- MAGIC SELECT weekday('2009-07-30');

-- COMMAND ----------

-- DBTITLE 1,Spark SQL Function Examples
SELECT 
ModelVariableId,
VariableName,

ltrim("V", VariableName) as VariableName_ltrim,  --ltril function
UpdatedBy,

rtrim("afe", UpdatedBy) as UpdatedBy_rtrim,  --rtrim

trim(BOTH "a" FROM UpdatedBy) as UpdatedBy_trim,  --trim

substring(VariableName, 4, 10) as VariableName_substring, --substring

regexp_replace(VariableName, "[0-9]", "-") AS VariableName_rexex_replace,  --regex_replace

to_timestamp('2020-10-14 00:12:00'), 

StartDate,
EndDate,

months_between(to_date(StartDate), to_date(EndDate))  --months_between
from nfaprod_mo_conformed.modelvariable
WHERE ModelVariableId = 420
and VariableName like '%VA\\PROD\\INVACCT_\\___\\GA_%'  -- like operator 
and UpdatedBy = 'shaferg'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC NOTE:
-- MAGIC 1. The string in '=' and 'like' operator must be case sensitive. 
-- MAGIC 2. If the string consists of special escape character '\' in the string value the replace the single '\' with double backslash '\\' as mentioned in cmd 11 line 24
-- MAGIC 3. When plotting graph using notebook visualization tool from **Plotting options** in the results. Click SHIFT + Apply to view the visualization.

-- COMMAND ----------

-- MAGIC %md [For more information on BuiltIn functions: Right click here to open it in new tab](https://docs.databricks.com/spark/2.x/spark-sql/language-manual/functions.html)

-- COMMAND ----------

show create table nfaprod_mo_views.vw_modelvalue_get 

-- COMMAND ----------

-- DBTITLE 1,Plotting - Bar Chart
-- MAGIC %sql
-- MAGIC SELECT Loadid, SrcSystemName, OuterProjMo, CommonVarName, SourceVarName, VariableValue from nfaprod_mo_views.vw_modelvalue_get where  LoadId in (84892, 84891)

-- COMMAND ----------

-- DBTITLE 1,Plotting - Line Chart
SELECT * from nfaprod_mo_views.vw_modelvalue_get where LoadId in (84892, 84891)
