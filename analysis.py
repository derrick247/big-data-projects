# Importer les bibliothèques nécessaires pour Spark et l'analyse k-means
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

# Initialiser une session Spark
spark = SparkSession.builder.appName("KMeansAnalysis").getOrCreate()

# Charger les données depuis le fichier JSON
data = spark.read.json("pollution.json")

# Sélectionner les colonnes pertinentes pour l'analyse k-means
# Ici, nous allons utiliser 'value' et les informations de 'period' (datetimeFrom et datetimeTo)
# Vous pouvez ajuster les colonnes sélectionnées en fonction de votre analyse
selected_data = data.select(
    col("value"),
    col("period.datetimeFrom.utc").alias("datetimeFrom_utc"),
    col("period.datetimeTo.utc").alias("datetimeTo_utc")
)

# Remplacer les valeurs nulles par 0 pour éviter les erreurs
selected_data = selected_data.na.fill(0)

# Assembler les caractéristiques en un seul vecteur
assembler = VectorAssembler(
    inputCols=["value", "datetimeFrom_utc", "datetimeTo_utc"],
    outputCol="features"
)
final_data = assembler.transform(selected_data)

# Initialiser l'algorithme k-means
# Vous pouvez ajuster le nombre de clusters (k) en fonction de votre analyse
kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster")

# Entraîner le modèle k-means
model = kmeans.fit(final_data)

# Obtenir les résultats du clustering
predictions = model.transform(final_data)

# Afficher les résultats
print("Résultats du clustering:")
predictions.select("value", "datetimeFrom_utc", "datetimeTo_utc", "cluster").show()

# Arrêter la session Spark
spark.stop()
