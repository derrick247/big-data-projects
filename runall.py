import subprocess
import time
import random

# 1. Génération d’un groupId Kafka unique pour éviter les collisions
random_group_id = f"pollution-group-{random.randint(1_000_000_000, 9_999_999_999)}"

print("⏳ Attente de 30 secondes pour ingestion initiale...")
time.sleep(30)

# 2. Lancer le producteur (récupère les données d'une API et les publie dans Kafka)
producer_process = subprocess.Popen(["py", "-3.10", "producer.py"])

# 3. Lancer le consommateur (écoute les données sur le topic Kafka et les enregistre dans HDFS)
consumer_process = subprocess.Popen(["py", "-3.10", "consumer.py", random_group_id])

# 4. Attendre un peu pour laisser le temps aux données d’arriver
print(f"📥 En attente des nouvelles données Kafka... (Group ID: {random_group_id})")
time.sleep(30)

# 5. Lancer l'analyse Spark avec KMeans
print("🚀 Lancement de l’analyse Spark avec KMeans...")
spark_process = subprocess.Popen(["C:\\Users\\donal\\Downloads\\spark-3.5.4-bin-hadoop3\\bin\\spark-submit.cmd", "analyse_spark.py"])

# 6. Attendre la fin des processus
producer_process.wait()
consumer_process.wait()
spark_process.wait()

print("✅ Pipeline Big Data exécuté avec succès.")
