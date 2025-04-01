from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

# Démarrer Spark
spark = SparkSession.builder.appName("PollutionData").getOrCreate()

# Charger les données texte depuis HDFS
rdd = spark.sparkContext.textFile("hdfs://localhost:9000/data/pollution_data.txt")

# Filtrer les lignes vides
rdd_filtered = rdd.filter(lambda line: line.strip() != "")

# Grouper chaque 4 lignes consécutives (car chaque enregistrement = 4 lignes)
grouped = rdd_filtered.zipWithIndex() \
    .map(lambda x: (x[1] // 4, x[0])) \
    .groupByKey() \
    .mapValues(list) \
    .map(lambda x: x[1])  # récupérer juste les blocs

# Parser chaque bloc pour en extraire les valeurs
parsed = grouped.map(lambda lines: (
    lines[0].split(":")[1].strip(),  # Paramètre
    float(lines[1].split(":")[1].strip().split()[0]),  # Valeur
    lines[2].split(":")[1].strip(),  # Première mesure
    lines[3].split(":")[1].strip(),  # Dernière mesure
))

# Convertir en DataFrame
df = parsed.toDF(["parametre", "valeur", "debut", "fin"])

# Afficher
df.show(truncate=False)
