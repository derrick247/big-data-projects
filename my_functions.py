import requests
import json

def get_sensors_id(data):
    sensor_id = ()
    for value in data['results']:
        for sensor in value['sensors']:
            sensor_id = sensor_id + (sensor['id'], )
    return list(sensor_id)

def get_measurements(id, URL, HEADERS):
    response = requests.get(f'{URL}/{id}/measurements?limit=5', headers=HEADERS)  
    if response.status_code == 200:
      return response.json()['results']
