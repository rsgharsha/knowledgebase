# Databricks notebook source
# MAGIC %md Assign 1

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

dbutils.fs.ls ("/FileStore/tables/listing_host_start.csv")
listing_host_start_df = pd.read_csv("/dbfs/FileStore/tables/listing_host_start.csv", header = 0, delimiter = ",")
#listing_host_start_df.head()
#print(listing_host_start_df['host_duration'])
#plt.boxplot(listing_host_start_df['host_duration'])
listing_host_start_df.shape[0]
listing_host_start_df_clean =listing_host_start_df.dropna(subset=['host_duration'])
listing_host_start_df_clean.shape[0]

plt.boxplot(listing_host_start_df_clean['host_duration'])
plt.show()

# COMMAND ----------

from scipy import stats
import numpy as np

iqr_va = stats.iqr(listing_host_start_df_clean['host_duration'], axis = 0)
q1 = np.percentile(listing_host_start_df_clean['host_duration'], 25, axis = 0) 
q3 = np.percentile(listing_host_start_df_clean['host_duration'], 75, axis = 0) 
upper_boundary = q3 + 1.5 * iqr_va
lower_boundary = q1 - (1.5 * iqr_va)
print(F"iqr_va val is: {iqr_va} \nq1 val is: {q1} \nq3 val is {q3} \nupper_boundary val is:{upper_boundary} \nlower_boundary val is:{lower_boundary}")


# COMMAND ----------

listing_host_start_df_clean_wo_outliers = listing_host_start_df_clean[listing_host_start_df_clean['host_duration'] < 3959]
listing_host_start_df_clean_wo_outliers.shape[0]
#np. std (listing_host_start_df_clean_wo_outliers['host_duration'] , ddof = 1) # sample standard deviation

# COMMAND ----------

mean_val = np.mean(listing_host_start_df_clean_wo_outliers['host_duration'])
median_val = np.median(listing_host_start_df_clean_wo_outliers['host_duration'])
std_val = np.std(listing_host_start_df_clean_wo_outliers['host_duration']) # population
#listing_host_start_df.shape[0]
skewness_stats = stats.skew(listing_host_start_df_clean_wo_outliers['host_duration'])
skewness_stats_wo = mean_val - median_val
print(F"mean_val val is: {mean_val} \nmedian_val val is: {median_val} \nstd_val val is {std_val} \nskewness_stats val is:{skewness_stats} \nskewness_stats_wo val is:{skewness_stats_wo}")

# COMMAND ----------

plt.hist(listing_host_start_df_clean_wo_outliers['host_duration'])

# COMMAND ----------

bin_width = 391
plt.hist(listing_host_start_df_clean_wo_outliers['host_duration'],bins = range (-5, 4000, bin_width ))

# COMMAND ----------

max = np.max(listing_host_start_df_clean_wo_outliers['host_duration'])
min = np.min(listing_host_start_df_clean_wo_outliers['host_duration'])
(max - min)/

# COMMAND ----------

plt.scatter(listing_host_start_df_clean['host_duration'], listing_host_start_df_clean['start_month'])
plt.show

# COMMAND ----------

df = spark.read.load("/FileStore/tables/listing_host_start.csv",format="csv", sep=",", inferSchema="true", header="true")
df.createOrReplaceTempView("vw_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from (
# MAGIC         select *, rank() over(order by cnt desc) as rnk
# MAGIC         from (
# MAGIC                 select start_month, count(*) as cnt
# MAGIC                 from vw_df 
# MAGIC                 group by start_month
# MAGIC         )
# MAGIC         order by cnt desc
# MAGIC )
# MAGIC where rnk <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select start_month, count(*) as cnt, rank() over(order by cnt desc) as rnk
# MAGIC                 from vw_df 
# MAGIC                 group by start_month, rank() over(order by cnt desc) as rnk

# COMMAND ----------

#reread = pd.read_hdf('./store.h5')
# Access  data  store
data_store = pd.HDFStore("/dbfs/FileStore/tables/Reviews4Cluster.h5")
# Retrieve  data  using  key
   #df_property = data_store[’preprocessed_property ’]
    #data_store.close ()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

reviews_df = pd.read_csv("/dbfs/FileStore/tables/reviews.csv", header = 0, delimiter = ",")
reviews_df_clean = reviews_df.dropna()
review_150_sample_df = reviews_df_clean.sample (150)

print("before clean up -",reviews_df.shape,",after clean up -",reviews_df_clean.shape, ",random sample of 150 -",review_150_sample_df.shape)
display(review_150_sample_df)

# COMMAND ----------

accuracy_cleanliness_150_sample_df = review_150_sample_df[['review_scores_accuracy','review_scores_cleanliness']]
#accuracy_cleanliness_150_sample_df = review_150_sample_df[['review_scores_accuracy','review_scores_cleanliness','review_scores_checkin']]
all_150_sample_df = review_150_sample_df.drop(['id','host_since'], axis=1)
display(accuracy_cleanliness_150_sample_df)
display(all_150_sample_df)
temp_df = all_150_sample_df

# COMMAND ----------

import scipy.cluster.hierarchy as sch
from sklearn.cluster import AgglomerativeClustering
# create dendrogram with data df_0
dendrogram = sch.dendrogram(sch.linkage(temp_df, method='ward'))
# create clusters
hc=AgglomerativeClustering(n_clusters=3, affinity='euclidean', linkage= 'ward')
# save clusters for chart
y_hc=hc.fit_predict ( temp_df )
# Here you can use different data for prediction

# COMMAND ----------

from sklearn.cluster import KMeans
wcss=[]
for i in range(1,11) :
  kmeans = KMeans (n_clusters=i, init='k-means++', random_state=42)
  kmeans.fit(temp_df)
  wcss.append(kmeans.inertia_)
plt.plot (range(1,11) , wcss )
plt.title ('K- Means')
plt.xlabel ('Number of clusters')
plt.ylabel ('WCSS')
plt.show ()

# COMMAND ----------

kmeans = KMeans(n_clusters = 3)
# fit kmeans object to data df_1
kmeans.fit( temp_df )
# print location of clusters learned by kmeans object
print(kmeans.cluster_centers_ )
# save new clusters for chart
y_km=kmeans.fit_predict (temp_df)

# COMMAND ----------

# DBTITLE 1,Assign 2 Ques 2
import pandas as pd
import matplotlib.pyplot as plt

Metro_Interstate_Traffic_AMPeak_df = pd.read_csv("/dbfs/FileStore/tables/Metro_Interstate_Traffic_AMPeak_cleaned.csv", header = 0, delimiter = ",")
Metro_Interstate_Traffic_AMPeak_df_clean = Metro_Interstate_Traffic_AMPeak_df.dropna()

print("before clean up -",Metro_Interstate_Traffic_AMPeak_df.shape,",after clean up -",Metro_Interstate_Traffic_AMPeak_df_clean.shape)
display(Metro_Interstate_Traffic_AMPeak_df)

# COMMAND ----------

Metro_Interstate_Traffic_AMPeak_df.weekend.unique()
Metro_Interstate_Traffic_AMPeak_df.columns.tolist()
#traffic volume= b0+b1(weekend)+b2(clouds)+b3 (temperature)+b4 (snowfall) (M1);
#traffic volume = b0 + b1(weekend) +b2(clouds)+b3(snowfall) (M2).

# COMMAND ----------

import statsmodels.formula.api as smf

model1 = smf.ols(formula = 'traffic_volume ~ weekend + clouds_all + temp + snow_1h', data = Metro_Interstate_Traffic_AMPeak_df)
model2 = smf.ols(formula = 'traffic_volume ~ weekend + clouds_all + snow_1h', data = Metro_Interstate_Traffic_AMPeak_df)
result_formula = model1.fit()
result_formula.summary()

# COMMAND ----------

result_formula = model2.fit()
result_formula.summary()
