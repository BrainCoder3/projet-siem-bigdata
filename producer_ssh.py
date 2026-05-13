import csv
import json
import time
import os
from kafka import KafkaProducer

# Configuration pour SSH
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'ssh-logs' # Topic différent pour ne pas mélanger les données
DATASET_PATH = 'data/OpenSSH_2k_log_structured.csv'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

if not os.path.exists(DATASET_PATH):
    print(f"Erreur : Le fichier {DATASET_PATH} est introuvable.")
    exit()

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=json_serializer
)

print(f"--- DÉMARRAGE DU PRODUCER SSH ---")

try:
    with open(DATASET_PATH, mode='r') as csv_file:
        reader = csv.DictReader(csv_file)
        for count, row in enumerate(reader, 1):
            # On envoie les logs SSH ici
            producer.send(TOPIC_NAME, value=row)
            
            time.sleep(0.1) # Un peu plus lent pour simuler des logs réels
            if count % 50 == 0:
                print(f">> {count} logs SSH envoyés...")
except Exception as e:
    print(f"Erreur : {e}")
finally:
    producer.flush()
