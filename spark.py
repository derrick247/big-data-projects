from pyspark.sql import SparkSession
from hdfs import InsecureClient
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import os
import pandas as pd

import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, stddev, abs as spark_abs
from pyspark.sql.types import FloatType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Configuration de l'environnement pour Python 3.10
os.environ["PYSPARK_PYTHON"] = r"D:\\big-data-projects\\.venv\\Scripts\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\\big-data-projects\\.venv\\Scripts\\python.exe"

# Initialiser la session Spark
spark = SparkSession.builder.appName("Pollution Anomaly Detection (KMeans)").getOrCreate()

# Lire le fichier local pollution_data.txt
local_path = "file:///D:/big-data-projects/spark.py"
rdd = spark.sparkContext.textFile(local_path)

print("\n\U0001F4C2 Aperçu brut du fichier pollution_data.txt :\n")
print("\n".join(rdd.take(12)))

# Nettoyer et regrouper les lignes en blocs
blocks = rdd.filter(lambda line: line.strip() != "").map(lambda line: line.strip())
grouped = blocks.zipWithIndex().map(lambda x: (x[1] // 4, x[0])).groupByKey().mapValues(list)

# Extraire les valeurs avec regex
def parse_block(block):
    try:
        param = re.search(r"Paramètre: (.+)", block[0]).group(1)
        value = float(re.search(r"Unité: ([0-9.]+)", block[1]).group(1))
        date_from = re.search(r"Première mesure: ([^\n]+)", block[2]).group(1)
        date_to = re.search(r"Dernière mesure: ([^\n]+)", block[3]).group(1)
        return (param, value, date_from, date_to)
    except:
        return None

rows = grouped.map(lambda x: parse_block(x[1])).filter(lambda x: x is not None)
df = spark.createDataFrame(rows, ["parameter", "value", "start_time", "end_time"])

print("\n\u2705 Aperçu des données formatées :")
df.show(5, truncate=False)

# Appliquer KMeans pour détection d'anomalies
assembler = VectorAssembler(inputCols=["value"], outputCol="features")
df_kmeans = assembler.transform(df)

kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(df_kmeans)
centers = model.clusterCenters()
df_clusters = model.transform(df_kmeans)

# Calculer la distance entre point et centroïde
def compute_distance(value, cluster_id):
    center = centers[cluster_id][0]
    return float(abs(value - center))

distance_udf = udf(compute_distance, FloatType())
df_clusters = df_clusters.withColumn("distance_to_center", distance_udf(col("value"), col("prediction")))

# Seuil d'anomalie = 95e percentile des distances
threshold = df_clusters.selectExpr("percentile(distance_to_center, 0.95)").collect()[0][0]
anomalies = df_clusters.filter(col("distance_to_center") > threshold)

print("\n\U0001F50E Anomalies détectées par KMeans (distance > 95e percentile) :")
anomalies.select("parameter", "value", "start_time", "end_time", "distance_to_center").show(truncate=False)

print(f"\n\U0001F9EE Nombre d'anomalies détectées : {anomalies.count()}")

# Sauvegarde dans HDFS
output_path = "hdfs://172.18.0.4:9000/data/anomalies_kmeans_pollution.txt"
anomalies.select("parameter", "value", "start_time", "end_time", "distance_to_center") \
    .write.mode("overwrite").csv(output_path, header=True)

print(f"\n\u2705 Fichier anomalies_kmeans_pollution.txt bien enregistré dans HDFS \u2714\ufe0f\n\u27a1\ufe0f {output_path}")

spark.stop()