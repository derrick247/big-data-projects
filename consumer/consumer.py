from kafka import KafkaConsumer
import json
import os
import subprocess
from hdfs import InsecureClient

# Connexion a HDFS
client = InsecureClient('http://hadoop-namenode:9870', user='root')

# Vérifier si le fichier existe sur la machine locale
if not os.path.exists("pollution.json"):
    open("pollution.json", "w").close()

# Connexion au topic "pollution"
consumer = KafkaConsumer(
    "pollution",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("📥 En attente des données Kafka...")

measurements = []
for message in consumer:
    measurements.append(message.value)  # Récupérer les données envoyées par Kafka
    
    # Écrire dans le fichier local
    with open("pollution.json", "w", encoding="utf-8") as file:
        json.dump(measurements, file, indent=4, ensure_ascii=False)

    try:
        client.delete("/data/pollution.json")
        print(f"📂 Fichier supprimé avec succès.")
        
        hdfs_path = '/data/pollution.json'  # Chemin HDFS de destination
        local_path = 'pollution.json'  # Chemin local du fichier
        client.upload(hdfs_path, local_path)
        print(f"🚀 Fichier uploadé avec succès.")

    except Exception as e:
        print(f"Erreur lors de l'upload du fichier : {e}")