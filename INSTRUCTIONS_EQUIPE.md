# Documentation Projet SIEM — Instructions pour l'équipe

## Ce qui est déjà fait (P1 - Chef projet)

### Infrastructure Docker
Tous les outils tournent sur ma machine via Docker :
- Kafka (port 9092)
- Zookeeper (port 2181)
- Spark (port 8080)
- MongoDB (port 27017)
- Elasticsearch (port 9200)
- Kibana (port 5601)

### Topics Kafka créés et remplis
- logs-systeme — 4001 messages (logs SSH)
- flux-reseau — 50326 messages (trafic réseau CICIDS)
- threat-intel — prêt

### Fichiers produits
- kafka_config.py
- producer.py

---

## Ce que chacun doit faire

### P2 — producer_live.py
Générer de faux logs d'attaques en temps réel pour la démo.

### P3 — spark_streaming.py
Lire depuis Kafka et détecter les attaques avec 3 modèles IA :
1. Random Forest
2. Isolation Forest
3. Réseau de neurones

Métriques à calculer : précision, rappel, F1-score, temps d'inférence.

### P4 — elasticsearch_config.py + mongodb_config.py
Recevoir les résultats de Spark et les stocker.

### P5 — Dashboard Kibana + rapport PDF

---

## Installation sur votre VM

### 1. Cloner le repo
```bash
git clone https://github.com/BrainCoder3/projet-siem-bigdata
cd projet-siem-bigdata
```

### 2. Installer les dépendances
```bash
pip install kafka-python pandas numpy scikit-learn tensorflow --break-system-packages
```

### 3. Lancer Docker
```bash
sudo usermod -aG docker $USER
newgrp docker
docker compose up -d
```

### 4. Vérifier Kafka
```bash
docker exec -it projet-siem-bigdata-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic logs-systeme

docker exec -it projet-siem-bigdata-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic flux-reseau
```

Résultat attendu :
- logs-systeme:0:4001
- flux-reseau:0:50326

### 5. Télécharger les datasets
```bash
mkdir data
wget -O data/OpenSSH_2k_log_structured.csv "https://raw.githubusercontent.com/logpai/loghub/refs/heads/master/OpenSSH/OpenSSH_2k.log_structured.csv"
```

Pour CICIDS 2017 :
```bash
pip install kaggle --break-system-packages
mkdir -p ~/.kaggle
# Créer token sur kaggle.com → Settings → API → Create New Token
echo '{"token":"VOTRE_TOKEN"}' > ~/.kaggle/access_token
chmod 600 ~/.kaggle/access_token
kaggle datasets download -d ericanacletoribeiro/cicids2017-cleaned-and-preprocessed -p data/ --unzip
head -1 data/cicids2017_cleaned.csv > data/cicids2017_sample.csv
tail -n +2 data/cicids2017_cleaned.csv | head -50000 >> data/cicids2017_sample.csv
```

### 6. Tester
```bash
python3 kafka_config.py
```

---

## Structure du repo
