# Datasets — à télécharger localement

## 1. OpenSSH (logs système - 350KB)
wget -O data/OpenSSH_2k_log_structured.csv "https://raw.githubusercontent.com/logpai/loghub/refs/heads/master/OpenSSH/OpenSSH_2k.log_structured.csv"

## 2. CICIDS 2017 (trafic réseau - 685MB)
kaggle datasets download -d ericanacletoribeiro/cicids2017-cleaned-and-preprocessed -p data/ --unzip
head -1 data/cicids2017_cleaned.csv > data/cicids2017_sample.csv
tail -n +2 data/cicids2017_cleaned.csv | head -50000 >> data/cicids2017_sample.csv

## Important
Ne jamais pusher ces fichiers sur GitHub.
