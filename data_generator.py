import json
import random
from datetime import datetime, timedelta
import time

# Fichier de sortie
OUTPUT_FILE = "/data/sample_data.jsonl"

# Fonction pour générer des CDRs et EDRs
def generate_voice_cdr():
    return {
        "record_type": "voice",
        "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat() + "Z",
        "caller_id": f"2126{random.randint(10000000, 99999999)}",
        "callee_id": f"{random.randint(1000000000, 1999999999)}",
        "duration_sec": random.randint(1, 600),
        "cell_id": f"ALHOCEIMA_{random.randint(1, 50):02}",
        "technology": random.choice(["2G", "3G", "4G"])
    }

def generate_sms_cdr():
    return {
        "record_type": "sms",
        "timestamp": datetime.now().isoformat() + "Z",
        "sender_id": f"2126{random.randint(10000000, 99999999)}",
        "receiver_id": f"2126{random.randint(10000000, 99999999)}",
        "cell_id": f"IMZOUREN_{random.randint(1, 30):02}",
        "technology": random.choice(["3G", "4G"])
    }

def generate_data_edr():
    return {
        "record_type": "data",
        "timestamp": datetime.now().isoformat() + "Z",
        "user_id": f"2126{random.randint(10000000, 99999999)}",
        "data_volume_mb": round(random.uniform(1, 1024), 2),
        "session_duration_sec": random.randint(60, 3600),
        "cell_id": f"BNIBOUFRAH_{random.randint(1, 20):02}",
        "technology": random.choice(["4G", "5G"])
    }

def generate_corrupted_record():
    return {
        "record_type": "corrupted",
        "timestamp": "2025-13-32T25:61:61Z",
        "caller_id": "NOT_A_NUMBER",
        "duration_sec": -120
    }

# Écrire les données dans un fichier en continu (ajustable)
while True:
    record_type = random.choices(
        ["voice", "sms", "data", "corrupted"],
        weights=[0.45, 0.25, 0.25, 0.05]
    )[0]

    if record_type == "voice":
        record = generate_voice_cdr()
    elif record_type == "sms":
        record = generate_sms_cdr()
    elif record_type == "data":
        record = generate_data_edr()
    else:
        record = generate_corrupted_record()

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
        print(f"Enregistrement écrit : {record}")

    time.sleep(5)  # Tu peux changer la fréquence ici
