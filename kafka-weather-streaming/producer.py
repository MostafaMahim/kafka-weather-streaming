import pandas as pd
import json
import time
from confluent_kafka import Producer
from config import KAFKA_CONFIG, RAW_TOPIC

# ── 1. Load clean dataset ─────────────────────────────────────
df = pd.read_csv("data/clean_weather.csv")
print(f"✅ Loaded {len(df)} rows from clean_weather.csv")

# ── 2. Create Kafka Producer ──────────────────────────────────
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to [{msg.topic()}] partition {msg.partition()} → {msg.value().decode('utf-8')[:60]}...")

# ── 3. Stream rows one by one ─────────────────────────────────
print(f"\n🚀 Starting to stream data to topic: {RAW_TOPIC}")
print("Press Ctrl+C to stop\n")

for i, row in df.iterrows():
    # Build the message
    message = {
        "row_id"     : int(i),
        "Temp"       : round(float(row["Temp"]), 2),
        "DewPoint"   : round(float(row["DewPoint"]), 2),
        "Humidity"   : round(float(row["Humidity"]), 2),
        "WindSpd"    : round(float(row["WindSpd"]), 2),
        "Pressure"   : round(float(row["Pressure"]), 2),
        "Visibility" : round(float(row["Visibility"]), 2),
        "Next_Temp"  : round(float(row["Next_Temp"]), 2),
    }

    # Send to Kafka
    producer.produce(
        RAW_TOPIC,
        key=str(i),
        value=json.dumps(message),
        callback=delivery_report
    )

    producer.poll(0)

    # Wait 1 second between rows (live streaming effect)
    time.sleep(1)

producer.flush()
print("\n✅ All rows sent!")