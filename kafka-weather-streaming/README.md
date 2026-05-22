# 🌤️ Real-Time Weather Prediction with Apache Kafka

**Course:** ENGR 5785G — Assignment 1  
**Dataset:** Weather Toronto/Oshawa — Environment Canada  
**Language:** Python  
**Streams Library:** kafka-python (Streams-style processor)  

---

## 📌 Overview

This project builds a real-time streaming pipeline using Apache Kafka and Confluent Cloud.
It reads hourly weather data row by row, streams it through Kafka, applies a 
Random Forest ML model to predict the next hour's temperature, and displays 
live predictions in the console.

---

## 🗂️ Project Structure

kafka-weather-streaming/
├── data/                   ← Weather CSV files (not pushed to GitHub)
├── model/                  ← Trained ML model (not pushed to GitHub)
├── producer.py             ← Reads dataset, publishes to raw-data topic
├── processor.py            ← Streams processor: consumes, runs ML, publishes
├── consumer.py             ← Output consumer: reads predictions, prints results
├── train_model.py          ← Offline ML model training script
├── prepare_data.py         ← Data preparation script
├── config.py               ← Kafka connection config (not pushed to GitHub)
├── requirements.txt        ← Python dependencies
└── README.md               ← This file

---

## 📊 Dataset

- **Source:** Environment Canada — https://climate.weather.gc.ca
- **Station:** TORONTO INTL A
- **Period:** January–April 2023 (hourly data)
- **Features used:** Temperature, Dew Point, Humidity, Wind Speed, Pressure, Visibility
- **Target:** Next hour's temperature (°C)

---

## 🤖 ML Model

- **Algorithm:** Random Forest Regressor (100 estimators)
- **Training:** Offline on full dataset using scikit-learn
- **Model file:** `model/weather_model.pkl`
- **Performance:**
  - Mean Absolute Error : **0.49 °C**
  - R² Score            : **0.9791**

---

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/kafka-weather-streaming.git
cd kafka-weather-streaming
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Kafka
Create a `config.py` file with your Confluent Cloud credentials:
```python
KAFKA_CONFIG = {
    'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVER',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'YOUR_API_KEY',
    'sasl.password': 'YOUR_API_SECRET',
}
RAW_TOPIC = 'raw-data'
PREDICTIONS_TOPIC = 'predictions'
```

### 4. Download the dataset
Download hourly weather CSVs from:
https://climate.weather.gc.ca/climate_data/bulk_data_e.html
Place them in the `data/` folder.

### 5. Train the ML model
```bash
python train_model.py
```

---

## ▶️ How to Run

Open **3 terminals** in the project folder and run in this order:

**Terminal 1 — Streams Processor:**
```bash
python processor.py
```

**Terminal 2 — Producer:**
```bash
python producer.py
```

**Terminal 3 — Output Consumer:**
```bash
python consumer.py
```

---

## 🎥 Video Demo

👉 [Watch the demo here](YOUR_VIDEO_LINK_HERE)

---

## 📦 Dependencies

See `requirements.txt`