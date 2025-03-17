from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("PollutionDataAnalysis") \
    .getOrCreate()

# Charger les données depuis HDFS
df = spark.read.text("hdfs://hadoop-master:9000/data/pollution_data.txt")

# Afficher les 5 premières lignes
df.show(5, truncate=False)
