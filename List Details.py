# Databricks notebook source
# MAGIC %md ## Dev

# COMMAND ----------

# DBTITLE 1,Jobs list
import requests

endpoint_url = "https://nationwide-prd.cloud.databricks.com/api/2.0/jobs/list"
head_auth  = {'Authorization': 'Bearer dapibd287db11eee7fba63f650048aec13bd'}

response = requests.get(endpoint_url, headers=head_auth)

jobs_lst = response.json()['jobs']

print(F"Job id | Job name: | Creator name | Cluster")

for i in jobs_lst:
  setsin = i['settings']
  if "nfadev" in setsin['name'].strip().lower():
    if 'existing_cluster_id' in setsin:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['existing_cluster_id']}")
    else:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['new_cluster']}")

# COMMAND ----------

# DBTITLE 1,Databases list
schema_nfa = spark.sql("show databases like 'nfadev*'").select('databaseName').collect()
cnt = 0
for i in schema_nfa:
  print(i.databaseName)
  cnt += 1
print(len(schema_nfa))
print(F"count is:{cnt}")

# COMMAND ----------

# DBTITLE 1,Tables list
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

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -n -X GET -H 'Authorization: Bearer dapic919ead3b565905a4c0f270d4fcc7a06' -H 'Content-Type: application/json' "https://nationwide-prd.cloud.databricks.com/api/2.0/jobs/get?job_id=28897"

# COMMAND ----------

import requests

endpoint_url = "https://nationwide-prd.cloud.databricks.com/api/2.0/clusters/list"
head_auth  = {'Authorization': 'Bearer dapic919ead3b565905a4c0f270d4fcc7a06'}
clst = ['']

response = requests.get(endpoint_url, headers=head_auth)

clusters_lst = response.json()['clusters']

print(F"Cluster id | Cluster name: | Creator name | JDBC port")

for i in clusters_lst:
  cid = i['cluster_id']
  cname = i['cluster_name']
  if "NFActuarial".lower() in cname.lower():
    if 'jdbc_port' in i:
      print(F"{cid} | {cname} | {i['creator_user_name']} | {i['jdbc_port']} | ")
    else:
      print(F"{cid} | {cname} | {i['creator_user_name']} |  | ")

#custom tags:{i['custom_tags']}
#
  #if cid in setsin['name'].strip().lower():
#    if 'existing_cluster_id' in setsin:
#      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['existing_cluster_id']}")
#    else:
#      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['new_cluster']}")

#print(jobs_lst)

# COMMAND ----------

for i in range(0,len(clusters_lst)):
#for i in clusters_lst:
  #print(type(clusters_lst[i]))
  #print(clusters_lst[i])
  print(clusters_lst[i].keys())
  #print(clusters_lst[i]['cluster_name'])
  for j in clusters_lst[i].keys():
    print(F"ele:{j}, ele_type:{type(j)}")
  if "NFActuarial".lower() in clusters_lst[i]['cluster_name'].lower():
    print(F"cluster id:{clusters_lst[i]['cluster_id']}, cluster name:{clusters_lst[i]['cluster_name']}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##Test

# COMMAND ----------

# DBTITLE 1,Jobs list Test
import requests

endpoint_url = "https://nationwide-prd.cloud.databricks.com/api/2.0/jobs/list"
head_auth  = {'Authorization': 'Bearer dapic919ead3b565905a4c0f270d4fcc7a06'}

response = requests.get(endpoint_url, headers=head_auth)

jobs_lst = response.json()['jobs']

print(F"Job id | Job name: | Creator name | Cluster")

for i in jobs_lst:
  setsin = i['settings']
  if "nfatest" in setsin['name'].strip().lower():
    if 'existing_cluster_id' in setsin:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['existing_cluster_id']}")
    else:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['new_cluster']}")

# COMMAND ----------

# DBTITLE 1,Database list -test
schema_nfa = spark.sql("show databases like 'nfatest*'").select('databaseName').collect()
cnt = 0
for i in schema_nfa:
  print(i.databaseName)
  cnt += 1
print(len(schema_nfa))
print(F"count is:{cnt}")

# COMMAND ----------

# DBTITLE 1,Tables list - test
from pyspark.sql.functions import concat, col, lit, lower

schema_nfa = spark.sql("show databases like 'nfatest*'").select('databaseName').collect()
tables_all = []
for i in schema_nfa:
  sc = str(i.databaseName)
  spark.sql("use " + sc)
  tables_sc = spark.sql("show tables").select(concat(col("database"), lit("."), col("tableName")).alias("name"))
  tables = [str(row.name) for row in tables_sc.collect()] 
  for t in tables:
    tables_all.append(t)
    print(t)

# COMMAND ----------

# MAGIC %md ##Prod

# COMMAND ----------

# DBTITLE 1,Jobs list Prod
import requests

endpoint_url = "https://nationwide-prd.cloud.databricks.com/api/2.0/jobs/list"
head_auth  = {'Authorization': 'Bearer dapic919ead3b565905a4c0f270d4fcc7a06'}

response = requests.get(endpoint_url, headers=head_auth)

jobs_lst = response.json()['jobs']

print(F"Job id | Job name: | Creator name | Cluster")

for i in jobs_lst:
  setsin = i['settings']
  if "nfaprod" in setsin['name'].strip().lower():
    if 'existing_cluster_id' in setsin:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['existing_cluster_id']}")
    else:
      print(F"{i['job_id']} | {setsin['name'].strip()} | {i['creator_user_name']} | {setsin['new_cluster']}")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Database list -Prod
schema_nfa = spark.sql("show databases like 'nfaprod*'").select('databaseName').collect()
cnt = 0
for i in schema_nfa:
  print(i.databaseName)
  cnt += 1
print(len(schema_nfa))
print(F"count is:{cnt}")

# COMMAND ----------

# DBTITLE 1,Tables list - Prod
from pyspark.sql.functions import concat, col, lit, lower

schema_nfa = spark.sql("show databases like 'nfaprod*'").select('databaseName').collect()
tables_all = []
for i in schema_nfa:
  sc = str(i.databaseName)
  spark.sql("use " + sc)
  tables_sc = spark.sql("show tables").select(concat(col("database"), lit("."), col("tableName")).alias("name"))
  tables = [str(row.name) for row in tables_sc.collect()] 
  for t in tables:
    tables_all.append(t)
    print(t)
