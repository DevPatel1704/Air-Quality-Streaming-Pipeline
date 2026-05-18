# Air Quality Real-Time Streaming Pipeline
### ENGR 5785G — Assignment 1

A real-time streaming application built with **Apache Kafka** and **Faust** (Python Streams API) that streams air quality sensor data, runs live ML inference, and outputs CO concentration level predictions.

---

## Dataset

**Air Quality — UCI Machine Learning Repository**
- **Source:** https://archive.ics.uci.edu/dataset/360/air+quality
- **Records:** 9,358 hourly averages (after cleaning: 7,674 valid rows)
- **Description:** Hourly averaged responses from a 5-sensor gas multisensor device deployed in an Italian city (March 2004 – February 2005), alongside certified ground-truth CO, NOx, NO₂, benzene, and NMHC measurements.
- **Citation:** Vito, S. (2008). Air Quality [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C59K5F

---

## Streams Library Used

**Option A: Python + Faust**
- Library: `faust-streaming` (Python equivalent of Kafka Streams)
- The `streams_processor.py` uses a `@app.agent()` decorator to define a Faust agent that consumes raw sensor events, applies the ML model, and produces prediction messages — **no plain consumer loop**.

---

## ML Model

| Property        | Value |
|-----------------|-------|
| Algorithm       | Random Forest Classifier |
| Task            | Multi-class classification (Low / Medium / High CO level) |
| Features used   | 11 sensor readings (PT08.S1, C6H6, PT08.S2, NOx, PT08.S3, NO2, PT08.S4, PT08.S5, Temperature, Relative Humidity, Absolute Humidity) |
| Target variable | CO(GT) binned into: Low (<1.5 mg/m³), Medium (1.5–4.0 mg/m³), High (>4.0 mg/m³) |
| Training set    | 6,139 rows (80%) |
| Test set        | 1,535 rows (20%) |
| **Accuracy**    | **89.71%** |
| **F1 Score**    | **0.8969 (weighted)** |

### Detailed Classification Report

```
              precision    recall  f1-score   support
         Low       0.88      0.80      0.83       162
      Medium       0.93      0.90      0.91       635
        High       0.88      0.92      0.90       738

    accuracy                           0.90      1535
   macro avg       0.89      0.87      0.88      1535
weighted avg       0.90      0.90      0.90      1535
```

---

## Project Structure

```
Project - 1/
├── data/
│   ├── AirQualityUCI.csv     # UCI Air Quality dataset (downloaded)
│   └── AirQualityUCI.xlsx    # Excel version (unused)
├── config.py                 # Kafka broker config (local or Confluent Cloud)
├── train_model.py            # Step 0: Offline ML training script
├── producer.py               # Step 1: Kafka producer (streams dataset at ~1 row/sec)
├── streams_processor.py      # Step 2: Faust streams processor (ML inference)
├── consumer.py               # Step 3: Output consumer (prints predictions)
├── model.joblib              # Saved trained model
├── requirements.txt          # Python dependencies
└── README.md
```

---

## Setup

### Prerequisites
- Python 3.9+
- Apache Kafka running locally **OR** a Confluent Cloud account

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Kafka Broker

**Option A — Local Kafka (default, no config needed):**
The app defaults to `localhost:9092`. Make sure Kafka is running locally.

Start Zookeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start Kafka broker:
```bash
bin/kafka-server-start.sh config/server.properties
```

**Option B — Confluent Cloud:**
Set the following environment variables before running:
```bash
set KAFKA_BROKER=<your-bootstrap-server>
set KAFKA_API_KEY=<your-api-key>
set KAFKA_API_SECRET=<your-api-secret>
```

### 3. Create Kafka Topics (Local Kafka only)

```bash
kafka-topics.sh --create --topic air-quality-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic air-quality-predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
*(Confluent Cloud: create topics via the web UI)*

### 4. Train the ML Model (Run Once)

```bash
python train_model.py
```

This trains the Random Forest Classifier on the Air Quality dataset and saves `model.joblib`.

---

## 🐳 Deployment & Containerization (Docker)

To simplify production deployment and avoid running multiple terminals manually, the entire real-time streaming pipeline is fully containerized using **Docker** and **Docker Compose**. 

This allows you to deploy and launch the **Producer, Faust Streams Processor, and Predictions Consumer** concurrently with a single command.

### 1. Build and Launch the Pipeline

From the project root directory, run:
```bash
docker-compose up --build
```

### 2. What Docker handles automatically:
1. **Self-Healing Model Training:** The container checks if `model.joblib` is present. If it is missing (since it's gitignored), it automatically runs `python train_model.py` to train the classifier and generate the serialized model file *before* booting up Faust.
2. **Orchestrated Startup Delays:** The producer and consumer wait a few seconds for the Faust Streams Processor to establish its connection to Confluent Cloud and synchronize partitions, preventing message loss.
3. **Graceful Logging:** Output from all 3 components is aggregated and printed to a single terminal with clear color-coded prefixes (`live-predictions-consumer`, `air-quality-producer`, `streams-processor`).

### 3. Stop the Pipeline

To stop the services and clean up containers, run:
```bash
docker-compose down
```

---

## How to Run Each Component (Manual Local Method)

Open **three separate terminals** side-by-side and run in order:

### Terminal 1 — Faust Streams Processor (start first)
```bash
faust -A streams_processor worker -l info
```
Or alternatively:
```bash
python streams_processor.py worker -l info
```

### Terminal 2 — Output Consumer
```bash
python consumer.py
```

### Terminal 3 — Producer (start last)
```bash
python producer.py
```

The producer will begin sending one row per second. You will see predictions appear in the consumer terminal in real time.

---

## Video Demo

[Link to video demo] — *(YouTube unlisted / Google Drive / OneDrive)*

---

## Topics Used

| Topic                    | Purpose                              |
|--------------------------|--------------------------------------|
| `air-quality-raw`        | Raw sensor data (JSON, 1 msg/sec)    |
| `air-quality-predictions`| ML predictions output (JSON)         |

---

## Sample Output

**Producer (Terminal 3):**
```
[   1] Sent row    0 | Date: 10/03/2004 02.00.00 | CO: 2.60 mg/m3 | Temp: 13.6C | Level: Medium
[   2] Sent row    1 | Date: 10/03/2004 03.00.00 | CO: 2.00 mg/m3 | Temp: 13.3C | Level: Medium
```

**Consumer (Terminal 2):**
```
------------------------------------------------------------
[   1] Row 0  10/03/2004 02.00.00
  CO Measured : 2.60 mg/m3
  Predicted   : Medium  (confidence: 87.3%)
  Actual      : Medium  [CORRECT]
  Conditions  : Temp 13.6C  |  Humidity 48.9%
```

---

## Dependencies

| Package           | Version  | Purpose                      |
|-------------------|----------|------------------------------|
| kafka-python      | 2.0.2+   | Kafka producer & consumer     |
| faust-streaming   | 0.10.14+ | Python Streams API            |
| scikit-learn      | 1.4.2+   | Random Forest Classifier      |
| joblib            | 1.4.2+   | Model serialization           |
| pandas            | 2.2.2+   | Data loading & preprocessing  |
| numpy             | 1.26.4+  | Numerical operations          |
| python-dotenv     | 1.0.1+   | Optional: .env file support   |
