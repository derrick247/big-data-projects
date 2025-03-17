from kafka import KafkaConsumer
import json
import os
import subprocess

# Vérifier si le fichier existe sur la machine locale
if not os.path.exists("pollution_data.txt"):
    open("pollution_data.txt", "w").close()

# Connexion au topic "pollution"
consumer = KafkaConsumer(
    "pollution",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="pollution-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("📥 En attente des données Kafka...")

for message in consumer:
    data = message.value  # Récupérer les données envoyées par Kafka
    print("📥 Données reçues depuis Kafka :", data)  # Debug

    # Vérifier si la clé 'parameter' existe dans le message
    if "parameter" in data and "value" in data and "period" in data:
        formatted_data = f"""
        🔬 Paramètre: {data['parameter']['name']}
        📊 Unité: {data['value']} {data['parameter']['units']}
        📅 Première mesure: {data['period']['datetimeFrom']['local']}
        📅 Dernière mesure: {data['period']['datetimeTo']['local']}
        """

        print(formatted_data)

        # Écrire dans le fichier local
        with open("pollution_data.txt", "a", encoding="utf-8") as file:
            file.write(formatted_data + "\n")
            file.flush()

        # Copie dans Docker puis envoi dans HDFS
        if os.path.exists("pollution_data.txt"):
            print("📂 Copie du fichier dans Docker...")
            subprocess.run(["docker", "cp", "pollution_data.txt", "hadoop-master:/tmp/pollution_data.txt"])

            print("🚀 Envoi du fichier dans HDFS...")
            subprocess.run([
                "docker", "exec", "-it", "hadoop-master",
                "hdfs", "dfs", "-appendToFile", "/tmp/pollution_data.txt", "/data/pollution_data.txt"
            ])
    else:
        print("⚠️ Données non exploitables reçues :", data)
