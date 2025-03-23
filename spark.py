from pyspark.sql import SparkSession
from hdfs import InsecureClient
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import os
import pandas as pd

# Créer une session Spark
spark = SparkSession.builder \
    .appName("PollutionDataAnalysis") \
    .getOrCreate()
os.environ["PYSPARK_PYTHON"] = "D:/big-data-projects/.venv/Scripts/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "D:/big-data-projects/.venv/Scripts/python.exe"

schema = StructType([
    StructField("value", FloatType(), True),
    StructField("flagInfo", StructType([
        StructField("hasFlags", BooleanType(), True)
    ]), True),
    StructField("parameter", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("units", StringType(), True),
        StructField("displayName", StringType(), True)
    ]), True),
    StructField("period", StructType([
        StructField("label", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("datetimeFrom", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True),
        StructField("datetimeTo", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True)
    ]), True),
    StructField("coordinates", StringType(), True),  # Null dans les données actuelles
    StructField("summary", StringType(), True),  # Null dans les données actuelles
    StructField("coverage", StructType([
        StructField("expectedCount", IntegerType(), True),
        StructField("expectedInterval", StringType(), True),
        StructField("observedCount", IntegerType(), True),
        StructField("observedInterval", StringType(), True),
        StructField("percentComplete", FloatType(), True),
        StructField("percentCoverage", FloatType(), True),
        StructField("datetimeFrom", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True),
        StructField("datetimeTo", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True)
    ]), True)
])

# Connexion au Namenode (modifie l'IP et le port si nécessaire)
client = InsecureClient('http://localhost:9870', user='root')

# Lire un fichier depuis HDFS
try:
    with client.read('/data/pollution.json', encoding='utf-8') as reader:
        df = json.loads(reader.read())
        df_spark = spark.createDataFrame(df, schema=schema)
        # df_pandas = pd.read_json(df)
        # print(df_pandas.head())
except Exception as e:
    print(f"Erreur : {e}")
