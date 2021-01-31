# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Registry of Open Datasets: GDELT
# MAGIC 
# MAGIC The GDELT project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, counts, themes, sources, emotions, quotes, images and events driving our global society every second of every day.
# MAGIC 
# MAGIC ## Global threators
# MAGIC 
# MAGIC Analyzing threats issued between countries and visualizing the aggressiveness as a network.
# MAGIC 
# MAGIC 
# MAGIC #### References
# MAGIC 
# MAGIC 
# MAGIC [GDELT project](<https://www.gdeltproject.org>)
# MAGIC 
# MAGIC [GDELT Events Data Schema](<https://www.gdeltproject.org/data/lookups/SQL.tablecreate.txt>)
# MAGIC 
# MAGIC 
# MAGIC <http://alvinalexander.com/source-code/scala-java-lang-nosuchmethoderror-compiler-message/>
# MAGIC 
# MAGIC <https://databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html>
# MAGIC 
# MAGIC <https://docs.databricks.com/spark/latest/graph-analysis/graphframes/graph-analysis-tutorial.html>

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ETL: define schema for reading the data from the s3 bucket

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- https://www.gdeltproject.org/data/lookups/SQL.tablecreate.txt
# MAGIC 
# MAGIC CREATE TABLE gdelt_2019_hist (
# MAGIC  GLOBALEVENTID int, 
# MAGIC  SQLDATE int , 
# MAGIC  MonthYear string , 
# MAGIC  Year string , 
# MAGIC  FractionDate double , 
# MAGIC  Actor1Code string , 
# MAGIC  Actor1Name string , 
# MAGIC  Actor1CountryCode string , 
# MAGIC  Actor1KnownGroupCode string , 
# MAGIC  Actor1EthnicCode string , 
# MAGIC  Actor1Religion1Code string , 
# MAGIC  Actor1Religion2Code string , 
# MAGIC  Actor1Type1Code string , 
# MAGIC  Actor1Type2Code string , 
# MAGIC  Actor1Type3Code string , 
# MAGIC  Actor2Code string , 
# MAGIC  Actor2Name string , 
# MAGIC  Actor2CountryCode string , 
# MAGIC  Actor2KnownGroupCode string , 
# MAGIC  Actor2EthnicCode string , 
# MAGIC  Actor2Religion1Code string , 
# MAGIC  Actor2Religion2Code string , 
# MAGIC  Actor2Type1Code string , 
# MAGIC  Actor2Type2Code string , 
# MAGIC  Actor2Type3Code string , 
# MAGIC  IsRootEvent int , 
# MAGIC  EventCode string , 
# MAGIC  EventBaseCode string , 
# MAGIC  EventRootCode string , 
# MAGIC  QuadClass int , 
# MAGIC  GoldsteinScale double , 
# MAGIC  NumMentions int , 
# MAGIC  NumSources int , 
# MAGIC  NumArticles int , 
# MAGIC  AvgTone double , 
# MAGIC  Actor1Geo_Type int , 
# MAGIC  Actor1Geo_FullName string , 
# MAGIC  Actor1Geo_CountryCode string , 
# MAGIC  Actor1Geo_ADM1Code string , 
# MAGIC  Actor1Geo_Lat float , 
# MAGIC  Actor1Geo_Long float , 
# MAGIC  Actor1Geo_FeatureID int , 
# MAGIC  Actor2Geo_Type int , 
# MAGIC  Actor2Geo_FullName string , 
# MAGIC  Actor2Geo_CountryCode string , 
# MAGIC  Actor2Geo_ADM1Code string , 
# MAGIC  Actor2Geo_Lat float , 
# MAGIC  Actor2Geo_Long float , 
# MAGIC  Actor2Geo_FeatureID int , 
# MAGIC  ActionGeo_Type int , 
# MAGIC  ActionGeo_FullName string , 
# MAGIC  ActionGeo_CountryCode string , 
# MAGIC  ActionGeo_ADM1Code string , 
# MAGIC  ActionGeo_Lat float , 
# MAGIC  ActionGeo_Long float , 
# MAGIC  ActionGeo_FeatureID int , 
# MAGIC  DATEADDED int
# MAGIC 
# MAGIC )

# COMMAND ----------

gdelt = spark.table("gdelt_2019_hist")
gdelt.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from the s3 bucket into a dataframe

# COMMAND ----------

df = spark.read.option("header", "false").format("csv").option('delimiter', '\t').load("s3://gdelt-open-data/events/2019*", schema=gdelt.schema)

# COMMAND ----------

display(df.groupBy("EventRootCode").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in GDELT event codes to extract threats from the events raw data
# MAGIC 
# MAGIC - Define edges and nodes of the graph based on country codes and number of threats emitted
# MAGIC 
# MAGIC - Store threat edges and nodes in database tables

# COMMAND ----------

import pandas as pd
eventcode_data_types = {'CAMEOEVENTCODE': str, 'EVENTDESCRIPTION': str}
eventcodes = pd.read_csv('https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt', sep="\t", dtype=eventcode_data_types)
eventcodes.head()

# COMMAND ----------

eventcodes.loc[eventcodes['CAMEOEVENTCODE']=='13']

# COMMAND ----------

threats_df = df.filter(df['EventRootCode'] == '13')

# COMMAND ----------

threatVertex = threats_df.select('Actor1Geo_FullName', 'Actor1Geo_CountryCode').distinct()
threatVertex = (threatVertex
  .withColumnRenamed("Actor1Geo_FullName", "name")
  .withColumnRenamed("Actor1Geo_CountryCode", "id")).where(col("id").isNotNull())
display(threatVertex)

# COMMAND ----------

threatEdges = (threats_df.select("Actor1Geo_CountryCode", "Actor2Geo_CountryCode", 'GoldsteinScale')
  .withColumnRenamed("Actor1Geo_CountryCode", "src")
  .withColumnRenamed("Actor2Geo_CountryCode", "dst")
              .withColumnRenamed("GoldsteinScale", "gscale")).where(col("src").isNotNull() & col("dst").isNotNull())


# COMMAND ----------

type(threatEdges)

# COMMAND ----------

display(threatEdges)

# COMMAND ----------

threatVertex.write.saveAsTable("threat_vertices")
threatEdges.write.saveAsTable("threat_edges_")

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

vertices = spark.table("threat_vertices").where(col("id").isNotNull())
edges = spark.table("threat_edges_").where(col("src").isNotNull() & col("dst").isNotNull() & (col("src") != col("dst")))

# COMMAND ----------


from graphframes import GraphFrame

threatGraph = GraphFrame(vertices, edges)
# GraphFrame?

# COMMAND ----------

topThreaters = (threatGraph
  .edges
  .groupBy("src", "dst")
  .count()
  .orderBy(desc("count"))
#   .limit(10)
)

display(topThreaters)

# COMMAND ----------

threaters_pdf = topThreaters.toPandas()
threaters_pdf.head()

# COMMAND ----------

len(threaters_pdf)

# COMMAND ----------

!pip install networkx

# COMMAND ----------

import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# COMMAND ----------

top_threaters_pdf = threaters_pdf.head(300)
top_threaters_pdf.head()

# COMMAND ----------

edges = [[src, dst] for src, dst in zip(top_threaters_pdf['src'], top_threaters_pdf['dst'])]
edges_labels = {(src, dst):cnt for src, dst, cnt in zip(top_threaters_pdf['src'], top_threaters_pdf['dst'], top_threaters_pdf['count'])}

# COMMAND ----------

G = nx.DiGraph()
G.add_edges_from(edges)
pos = nx.circular_layout(G)
width = top_threaters_pdf['count'].values/top_threaters_pdf['count'].mean()*0.5

fig = plt.figure(figsize=(25, 25))    
nx.draw(G, pos, edge_color='black', width=width, linewidths=0.3,
        node_size=600,node_color='lightblue',alpha=0.9,
        labels={node:node for node in G.nodes()})

nx.draw_networkx_edge_labels(G, pos, edge_labels=edges_labels, font_color='red', font_size=6)
plt.axis('off')


display(fig)

# COMMAND ----------


