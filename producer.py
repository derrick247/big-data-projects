from kafka import KafkaProducer
import requests
import json
import time

# Remplace par TA NOUVELLE CLÉ API
API_KEY = "cbe86366906b474013ca66d5156808b4bd3fec4e4f5a96c2da8025efba72ca80"

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# URL pour récupérer les dernières mesures
API_URL = "https://api.openaq.org/v3/sensors/3917/measurements"

# Ajouter l'API Key dans les headers
HEADERS = {
    "X-API-Key": API_KEY
}

while True:
    try:
        response = requests.get(API_URL, headers=HEADERS)  
        if response.status_code == 200:
            data = response.json()  
            print(f"📤 Réponse API brute : {json.dumps(data, indent=2)}")  

            if 'results' in data:
                for record in data['results']:
                    producer.send("pollution", record)  
                    print(f"📤 Envoyé à Kafka : {record}")
            else:
                print("⚠️ Aucune donnée trouvée dans la réponse.")

        else:
            print(f"❌ Erreur API : {response.status_code} - {response.text}")

    except Exception as e:
        print(f"🚨 Erreur de connexion : {e}")

    time.sleep(10)
