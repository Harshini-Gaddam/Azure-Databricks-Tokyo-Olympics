# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

# Mounting azure data lake storage on the databricks cluster
# Used app registrations to get credentials to connect Databricks to ADLS (to get Client ID and Tenant ID)
# Using Connections & secrets, got secret key value

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "XXX",
"fs.azure.account.oauth2.client.secret": 'XXX',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/XXX(tanent_id)/oauth2/token"}


dbutils.fs.mount(
source = "abfss://XXX(containername)@XXX(storagename).dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

# Usually, to write spark code, we need to create sparksession, appname. 
# But in databricks, we don't have to create spark session from scratch. We will already have it.
spark

# COMMAND ----------

# Reading athletes data csv file using spark
athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/athletes.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# COMMAND ----------

coaches.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

# In entriesgender dataset, female, male, total has to be integer type. But we can see they are in the string format
# Changing column format of some columns
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

# using inferschema as true, it automatically detects the column type and gives the schema taking the values in the column into account.
# Instead of modifying each column manually, option inferschema gives the schema (by checking each column)
medals = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Analyzing the data**

# COMMAND ----------

# MAGIC %md
# MAGIC Find the top countries with the highest number of gold medals

# COMMAND ----------

top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry", "Gold").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate average number of entries by gender for each discipline

# COMMAND ----------

from pyspark.sql.functions import col

average_entries_by_gender = entriesgender \
    .withColumn('Avg_Female', col('Female') / col('Total')) \
    .withColumn('Avg_Male', col('Male') / col('Total'))

average_entries_by_gender.show()


# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/athletes")

# COMMAND ----------

entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/coaches")
medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/teams")

# COMMAND ----------


