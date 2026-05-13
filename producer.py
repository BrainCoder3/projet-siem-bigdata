import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoi OpenSSH dans logs-systeme
print("Envoi logs OpenSSH...")
df_ssh = pd.read_csv('data/OpenSSH_2k_log_structured.csv')
for _, row in df_ssh.iterrows():
    producer.send('logs-systeme', row.to_dict())
    time.sleep(0.01)
print(f"{len(df_ssh)} logs SSH envoyés !")

# Envoi CICIDS dans flux-reseau
print("Envoi trafic réseau CICIDS...")
df_cicids = pd.read_csv('data/cicids2017_sample.csv')
for _, row in df_cicids.iterrows():
    producer.send('flux-reseau', row.to_dict())
    time.sleep(0.01)
print(f"{len(df_cicids)} flux réseau envoyés !")

producer.flush()
print("Tous les données sont dans Kafka !")
