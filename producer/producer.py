from kafka import KafkaProducer
import requests
import json
import time
from my_functions import get_measurements

# Remplace par TA NOUVELLE CLÉ API
API_KEY = "cbe86366906b474013ca66d5156808b4bd3fec4e4f5a96c2da8025efba72ca80"

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MEASUREMENTS_URL = "https://api.openaq.org/v3/sensors"

# URL pour récupérer les dernières mesures
LOCATIONS_URL = "https://api.openaq.org/v3/locations?countries_id=22&&limit=20" # toutes les localisations en france

# Ajouter l'API Key dans les headers
HEADERS = {
    "X-API-Key": API_KEY
}

SENSOR_ID = [5561, 15370, 5566, 5567, 5569, 5568, 5572, 5574, 5573, 4275113, 5578, 5579, 5656, 5580, 5581, 5582, 4275139, 5609, 5622, 5583, 5614]
OTHER_SENSOR_FOR_FRANCE = [5584, 4274933, 5585, 5590, 5595, 4275255, 8537, 8536, 8535, 8534, 5586, 7586, 4274338, 5587, 5603, 4274492, 5620, 5588, 5589, 5593, 5597, 5591, 5621, 4275074, 5624, 5592, 5617, 24880, 5626, 5594, 4275409, 5601, 5599, 5596, 5602, 5600, 5598]

for id in SENSOR_ID:
    print("Measurement of sensors ", id)
    measurements = get_measurements(id, MEASUREMENTS_URL, HEADERS)
    
    for measurement in measurements:
        print("Send Measurements to kafka ...")
        producer.send("pollution", measurement)
    
    time.sleep(1)
