import json
import joblib
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_CONFIG, RAW_TOPIC, PREDICTIONS_TOPIC

# ── 1. Build kafka-python style config ───────────────────────
KAFKA_COMMON = {
    'bootstrap_servers'         : KAFKA_CONFIG['bootstrap.servers'],
    'security_protocol'         : 'SASL_SSL',
    'sasl_mechanism'            : 'PLAIN',
    'sasl_plain_username'       : KAFKA_CONFIG['sasl.username'],
    'sasl_plain_password'       : KAFKA_CONFIG['sasl.password'],
}

# ── 2. Load pre-trained ML model ─────────────────────────────
model = joblib.load("model/weather_model.pkl")
print("✅ ML model loaded successfully!")

# ── 3. Create Consumer (reads from raw-data) ──────────────────
consumer = KafkaConsumer(
    RAW_TOPIC,
    **KAFKA_COMMON,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='weather-processor-group',
)

# ── 4. Create Producer (writes to predictions) ───────────────
producer = KafkaProducer(
    **KAFKA_COMMON,
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
)

print(f"🚀 Streams Processor running...")
print(f"   Consuming from : {RAW_TOPIC}")
print(f"   Publishing to  : {PREDICTIONS_TOPIC}")
print(f"   Press Ctrl+C to stop\n")

# ── 5. Stream processing loop (Streams-style agent) ──────────
try:
    for message in consumer:
        record = message.value

        # Build feature array
        features = np.array([[
            record['Temp'],
            record['DewPoint'],
            record['Humidity'],
            record['WindSpd'],
            record['Pressure'],
            record['Visibility'],
        ]])

        # Run ML prediction
        predicted_temp = round(float(model.predict(features)[0]), 2)

        # Build result
        result = {
            "row_id"           : record['row_id'],
            "current_temp"     : record['Temp'],
            "predicted_temp"   : predicted_temp,
            "actual_next_temp" : record['Next_Temp'],
            "error"            : round(abs(predicted_temp - record['Next_Temp']), 2),
        }

        # Send to predictions topic
        producer.send(PREDICTIONS_TOPIC, value=result)
        producer.flush()

        print(f"🌡️  Row {record['row_id']:4d} | "
              f"Current: {record['Temp']:6.2f}°C | "
              f"Predicted: {predicted_temp:6.2f}°C | "
              f"Actual: {record['Next_Temp']:6.2f}°C | "
              f"Error: {result['error']:4.2f}°C")

except KeyboardInterrupt:
    print("\n✅ Processor stopped.")
finally:
    consumer.close()
    producer.close()