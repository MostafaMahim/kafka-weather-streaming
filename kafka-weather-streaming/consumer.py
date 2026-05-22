import json
from kafka import KafkaConsumer
from config import KAFKA_CONFIG, PREDICTIONS_TOPIC

# ── 1. Build kafka-python style config ───────────────────────
consumer = KafkaConsumer(
    PREDICTIONS_TOPIC,
    bootstrap_servers         = KAFKA_CONFIG['bootstrap.servers'],
    security_protocol         = 'SASL_SSL',
    sasl_mechanism            = 'PLAIN',
    sasl_plain_username       = KAFKA_CONFIG['sasl.username'],
    sasl_plain_password       = KAFKA_CONFIG['sasl.password'],
    value_deserializer        = lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset         = 'latest',
    group_id                  = 'weather-output-group',
)

print("=" * 60)
print("   🌤️  WEATHER PREDICTION OUTPUT CONSUMER")
print("=" * 60)
print(f"   Listening on topic: {PREDICTIONS_TOPIC}")
print("=" * 60)
print()

# ── 2. Print predictions as they arrive ───────────────────────
try:
    for message in consumer:
        result = message.value

        print(f"📨 New Prediction Received!")
        print(f"   Row ID         : {result['row_id']}")
        print(f"   Current Temp   : {result['current_temp']} °C")
        print(f"   Predicted Temp : {result['predicted_temp']} °C  ← ML Model")
        print(f"   Actual Temp    : {result['actual_next_temp']} °C")
        print(f"   Error          : {result['error']} °C")
        print("-" * 60)

except KeyboardInterrupt:
    print("\n✅ Consumer stopped.")
finally:
    consumer.close()