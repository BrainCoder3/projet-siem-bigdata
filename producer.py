import csv
import json
import time
from kafka import KafkaProducer

# Configuration dynamique
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'network-traffic'
DATASET_PATH = 'data/cicids2017_sample.csv'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Initialisation du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=json_serializer
)

print(f"--- DÉMARRAGE DU PRODUCER SIEM ---")
print(f"Envoi des données vers le topic: {TOPIC_NAME}")

try:
    with open(DATASET_PATH, mode='r') as csv_file:
        # DictReader utilise la 1ère ligne du CSV (headers) pour créer les clés JSON
        reader = csv.DictReader(csv_file)
        
        count = 0
        for row in reader:
            # Envoi de la ligne de trafic réseau
            producer.send(TOPIC_NAME, value=row)
            
            count += 1
            # Simulation temps réel : 0.05s de pause entre chaque ligne
            time.sleep(0.05)
            
            if count % 100 == 0:
                print(f">> {count} lignes de trafic envoyées...")

except FileNotFoundError:
    print(f"Erreur : Le fichier {DATASET_PATH} n'existe pas.")
except Exception as e:
    print(f"Erreur lors de l'envoi : {e}")
finally:
    producer.flush()
    print("Flux terminé proprement.")
