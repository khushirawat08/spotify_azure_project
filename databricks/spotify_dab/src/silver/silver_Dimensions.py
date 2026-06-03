# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import os 
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from transformation import Reusable

# COMMAND ----------

df=spark.read.format("parquet")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

df_user= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimUser/checkpoint")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/DimUser")

# COMMAND ----------

df_user=df_user.withColumn("user_name",upper(col("user_name")))

# COMMAND ----------

df_user_obj=Reusable()

df_user=df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user=df_user.dropDuplicates(['user_id'])



# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@yazurestrg.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md DimArtist

# COMMAND ----------

df_art= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimArtist/checkpoint")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_art_obj=Reusable()

df_art=df_art_obj.dropColumns(df_art,['_rescued_data'])


# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@yazurestrg.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md DimTrack

# COMMAND ----------

df_track= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimTrack/checkpoint")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track=df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
    .when(col('duration_Sec')<300,"medium")\
        .otherwise("high"))
df_track=df_track.withColumn("track_name",regexp_replace(col('track_name'),'-',' '))

# COMMAND ----------

df_track_obj=Reusable()

df_track=df_track_obj.dropColumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@yazurestrg.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md DimDate

# COMMAND ----------

df_date= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimDate/checkpoint")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@yazurestrg.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@yazurestrg.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md FactStream

# COMMAND ----------

df_fact= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@yazurestrg.dfs.core.windows.net/FactStream/checkpoint")\
    .load("abfss://bronze@yazurestrg.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@yazurestrg.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@yazurestrg.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

