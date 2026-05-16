"""
spark_ml/elasticsearch_sender.py
==================================
Lit depuis Kafka (flux-reseau), applique les 3 modèles IA,
et envoie les alertes ATTACK vers Elasticsearch.

Prérequis :
  - python3 preprocessing.py        (artefacts dans artifacts/)
  - python3 train_random_forest.py  (artifacts/models/)
  - python3 train_isolation_forest.py
  - python3 train_neural_network.py
  - Kafka + Elasticsearch qui tournent

Lancement :
  python3 elasticsearch_sender.py
"""

import os
import json
import pickle
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

# ─────────────────── CONFIG SPARK ────────────────────────────────
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType
)

# ─────────────────── CONFIG ───────────────────────────────────────
ARTIFACTS_DIR = Path("./artifacts")
MODELS_DIR    = ARTIFACTS_DIR / "models"
KAFKA_BROKER  = "localhost:9092"
KAFKA_TOPIC   = "flux-reseau"
ES_HOST       = "localhost"
ES_PORT       = 9200
ES_INDEX      = "siem-alertes"

# ─────────────────── SCHÉMA JSON UNSW-NB15 ───────────────────────
SCHEMA = StructType([
    StructField("id",                LongType(),   True),
    StructField("dur",               DoubleType(), True),
    StructField("proto",             StringType(), True),
    StructField("service",           StringType(), True),
    StructField("state",             StringType(), True),
    StructField("spkts",             LongType(),   True),
    StructField("dpkts",             LongType(),   True),
    StructField("sbytes",            LongType(),   True),
    StructField("dbytes",            LongType(),   True),
    StructField("rate",              DoubleType(), True),
    StructField("sttl",              LongType(),   True),
    StructField("dttl",              LongType(),   True),
    StructField("sload",             DoubleType(), True),
    StructField("dload",             DoubleType(), True),
    StructField("sloss",             LongType(),   True),
    StructField("dloss",             LongType(),   True),
    StructField("sinpkt",            DoubleType(), True),
    StructField("dinpkt",            DoubleType(), True),
    StructField("sjit",              DoubleType(), True),
    StructField("djit",              DoubleType(), True),
    StructField("swin",              LongType(),   True),
    StructField("stcpb",             LongType(),   True),
    StructField("dtcpb",             LongType(),   True),
    StructField("dwin",              LongType(),   True),
    StructField("tcprtt",            DoubleType(), True),
    StructField("synack",            DoubleType(), True),
    StructField("ackdat",            DoubleType(), True),
    StructField("smean",             LongType(),   True),
    StructField("dmean",             LongType(),   True),
    StructField("trans_depth",       LongType(),   True),
    StructField("response_body_len", LongType(),   True),
    StructField("ct_srv_src",        LongType(),   True),
    StructField("ct_state_ttl",      LongType(),   True),
    StructField("ct_dst_ltm",        LongType(),   True),
    StructField("ct_src_dport_ltm",  LongType(),   True),
    StructField("ct_dst_sport_ltm",  LongType(),   True),
    StructField("ct_dst_src_ltm",    LongType(),   True),
    StructField("is_ftp_login",      LongType(),   True),
    StructField("ct_ftp_cmd",        LongType(),   True),
    StructField("ct_flw_http_mthd",  LongType(),   True),
    StructField("ct_src_ltm",        LongType(),   True),
    StructField("ct_srv_dst",        LongType(),   True),
    StructField("is_sm_ips_ports",   LongType(),   True),
    StructField("attack_cat",        StringType(), True),
    StructField("label",             LongType(),   True),
])

# ─────────────────── CHARGEMENT ──────────────────────────────────
def load_artifacts():
    with open(ARTIFACTS_DIR / "scaler.pkl", "rb") as f:
        scaler = pickle.load(f)
    with open(ARTIFACTS_DIR / "feature_names.json") as f:
        feature_names = json.load(f)
    with open(ARTIFACTS_DIR / "cat_encoders.pkl", "rb") as f:
        cat_encoders = pickle.load(f)
    with open(ARTIFACTS_DIR / "clip_params.json") as f:
        clip_params = json.load(f)
    return scaler, feature_names, cat_encoders, clip_params


def load_models():
    models = {}
    for name, fname in [
        ("RandomForest",    "random_forest.pkl"),
        ("IsolationForest", "isolation_forest.pkl"),
        ("NeuralNetwork",   "neural_network.pkl"),
    ]:
        path = MODELS_DIR / fname
        if path.exists():
            with open(path, "rb") as f:
                obj = pickle.load(f)
            models[name] = obj if isinstance(obj, dict) and "model" in obj \
                           else {"model": obj, "threshold": None}
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name} absent (lancer train_{fname.replace('.pkl','')}.py)")
    return models


# ─────────────────── PRÉTRAITEMENT ───────────────────────────────
def preprocess(pdf, scaler, feature_names, cat_encoders, clip_params):
    df = pdf.copy()
    df = df.drop(columns=["id", "attack_cat", "label"], errors="ignore")

    for col in ["proto", "service", "state"]:
        le = cat_encoders.get(col)
        if le is None or col not in df.columns:
            continue
        most_common = le.classes_[0]
        df[col] = df[col].astype(str).apply(
            lambda v: v if v in le.classes_ else most_common
        )
        df[col] = le.transform(df[col])

    df = df.apply(pd.to_numeric, errors="coerce").fillna(0)

    for col in feature_names:
        if col not in df.columns:
            df[col] = 0.0
    df = df[feature_names]

    means = np.array(clip_params["means"])
    stds  = np.array(clip_params["stds"])
    stds[stds == 0] = 1.0
    X = np.clip(df.values.astype(np.float32),
                means - 10 * stds, means + 10 * stds)
    return scaler.transform(X)


# ─────────────────── PRÉDICTION ──────────────────────────────────
def predict(X, models):
    results = {}
    for name, obj in models.items():
        model, threshold = obj["model"], obj.get("threshold")
        try:
            if name == "IsolationForest":
                scores = -model.score_samples(X)
                preds  = (scores >= threshold).astype(int)
                probas = scores
            else:
                preds  = model.predict(X)
                probas = model.predict_proba(X)[:, 1]
            results[name] = {"preds": preds, "probas": probas}
        except Exception as e:
            print(f"  ⚠ {name} : {e}")
    return results


# ─────────────────── ELASTICSEARCH ───────────────────────────────
def init_es():
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(
            [{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}],
            request_timeout=10,
        )
        if not es.ping():
            print(f"  ⚠ Elasticsearch non joignable sur {ES_HOST}:{ES_PORT}")
            return None

        if not es.indices.exists(index=ES_INDEX):
            es.indices.create(index=ES_INDEX, body={
                "mappings": {"properties": {
                    "timestamp":  {"type": "date"},
                    "verdict":    {"type": "keyword"},
                    "proto":      {"type": "keyword"},
                    "service":    {"type": "keyword"},
                    "state":      {"type": "keyword"},
                    "attack_cat": {"type": "keyword"},
                    "confidence": {"type": "float"},
                    "rf_pred":    {"type": "integer"},
                    "rf_proba":   {"type": "float"},
                    "if_pred":    {"type": "integer"},
                    "if_score":   {"type": "float"},
                    "nn_pred":    {"type": "integer"},
                    "nn_proba":   {"type": "float"},
                    "sbytes":     {"type": "long"},
                    "dbytes":     {"type": "long"},
                    "rate":       {"type": "float"},
                    "dur":        {"type": "float"},
                }}
            })
            print(f"  ✓ Index '{ES_INDEX}' créé")
        else:
            print(f"  ✓ Index '{ES_INDEX}' existant")
        return es

    except ImportError:
        print("  ⚠ elasticsearch non installé → pip install elasticsearch")
        return None
    except Exception as e:
        print(f"  ⚠ Erreur ES : {e}")
        return None


def send_to_es(es, docs):
    if not es or not docs:
        return
    try:
        from elasticsearch.helpers import bulk
        bulk(es, [{"_index": ES_INDEX, "_source": d} for d in docs],
             raise_on_error=False)
    except Exception as e:
        print(f"  ⚠ Erreur envoi ES : {e}")


# ─────────────────── TRAITEMENT PAR BATCH ────────────────────────
def make_processor(models, scaler, feature_names, cat_encoders, clip_params, es):

    def process(batch_df, batch_id):
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        n = len(pdf)
        print(f"\n[Batch {batch_id}] {n} flux reçus")

        # Prétraitement
        try:
            X = preprocess(pdf, scaler, feature_names, cat_encoders, clip_params)
        except Exception as e:
            print(f"  ✗ Prétraitement : {e}")
            return

        # Prédictions
        results = predict(X, models)
        if not results:
            return

        # Vote majoritaire (égalité → ATTACK par prudence SIEM)
        votes = sum(results[m]["preds"] for m in results)
        final = (votes >= len(results) / 2).astype(int)

        n_attack = int(final.sum())
        n_normal = n - n_attack
        print(f"  → NORMAL: {n_normal}  ATTACK: {n_attack}")
        for name, res in results.items():
            print(f"     {name:<20} attacks={res['preds'].sum():3d}  "
                  f"avg_score={np.mean(res['probas']):.3f}")

        # Construction des documents Elasticsearch
        docs = []
        for i in range(n):
            if final[i] == 0:
                continue  # On n'indexe que les ATTACKS

            row = pdf.iloc[i]

            # Confiance = moyenne des probas des modèles supervisés
            sup_probas = [results[m]["probas"][i]
                          for m in results if m != "IsolationForest"]
            confidence = float(np.mean(sup_probas)) if sup_probas else 0.0

            doc = {
                "timestamp":  datetime.utcnow().isoformat(),
                "flow_id":    int(row.get("id", i)),
                "verdict":    "ATTACK",
                "confidence": round(confidence, 4),
                "votes":      int(votes[i]),
                "proto":      str(row.get("proto", "")),
                "service":    str(row.get("service", "")),
                "state":      str(row.get("state", "")),
                "attack_cat": str(row.get("attack_cat", "Unknown")),
                "real_label": int(row.get("label", -1)),
                "dur":        float(row.get("dur", 0)),
                "sbytes":     int(row.get("sbytes", 0)),
                "dbytes":     int(row.get("dbytes", 0)),
                "rate":       float(row.get("rate", 0)),
            }

            # Prédictions individuelles
            for name, es_pred, es_score in [
                ("RandomForest",    "rf_pred",  "rf_proba"),
                ("IsolationForest", "if_pred",  "if_score"),
                ("NeuralNetwork",   "nn_pred",  "nn_proba"),
            ]:
                if name in results:
                    doc[es_pred]  = int(results[name]["preds"][i])
                    doc[es_score] = round(float(results[name]["probas"][i]), 4)

            docs.append(doc)

        # Envoi vers Elasticsearch
        if docs:
            send_to_es(es, docs)
            print(f"  ✓ {len(docs)} alertes envoyées → Elasticsearch [{ES_INDEX}]")
        else:
            print(f"  ✓ Aucune attaque détectée dans ce batch")

    return process


# ─────────────────────────── MAIN ────────────────────────────────
if __name__ == "__main__":

    print("=" * 60)
    print("  SIEM — Spark + Modèles IA + Elasticsearch")
    print("=" * 60)
    print(f"  Kafka  : {KAFKA_BROKER}  topic: {KAFKA_TOPIC}")
    print(f"  ES     : {ES_HOST}:{ES_PORT}  index: {ES_INDEX}")
    print("=" * 60)

    # Vérification artefacts
    for f in ["scaler.pkl", "feature_names.json",
              "cat_encoders.pkl", "clip_params.json"]:
        if not (ARTIFACTS_DIR / f).exists():
            print(f"\n❌ Artefact manquant : {f}")
            print("   → Lancez d'abord : python3 preprocessing.py")
            exit(1)

    print("\n[INIT] Chargement des artefacts...")
    scaler, feature_names, cat_encoders, clip_params = load_artifacts()

    print("[INIT] Chargement des modèles...")
    models = load_models()

    if not models:
        print("\n❌ Aucun modèle trouvé. Lancez les scripts train_*.py d'abord.")
        exit(1)

    print("\n[INIT] Connexion Elasticsearch...")
    es = init_es()
    if es is None:
        print("  [INFO] Mode console uniquement (ES indisponible)")

    # Spark Session
    spark = SparkSession.builder \
        .appName("SIEM-ML-Elasticsearch") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("\n[SPARK] Session démarrée")

    # Lecture Kafka
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 500) \
        .load()

    # Parsing JSON
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*")

    # Pipeline IA + ES
    processor = make_processor(
        models, scaler, feature_names, cat_encoders, clip_params, es
    )

    query = parsed.writeStream \
        .foreachBatch(processor) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "./checkpoint_es") \
        .start()

    print(f"\n[SPARK] Pipeline actif — topic '{KAFKA_TOPIC}'")
    print(f"        Trigger toutes les 5 secondes | Ctrl+C pour arrêter\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STOP] Arrêt...")
        query.stop()
        spark.stop()
        print("[STOP] Terminé proprement.")
