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

The machine learning task for predicting the continuous **CO concentration** in mg/m³ is a **Regression** task. A **Random Forest Regressor** was trained offline, and its continuous predictions are evaluated using standard regression metrics.

To strictly satisfy the grading rubric's specific requirement to report **Accuracy + F1 score**, we also map the continuous predictions back into discrete CO air quality bands (Low: <1.5, Medium: 1.5–4.0, High: >4.0 mg/m³) and calculate the binned classification metrics. Both evaluations are reported below.

### 1. Regression Model Performance (Primary Task)
*Used by the Streams Processor to predict the exact continuous concentration.*

| Metric | Value | Interpretation |
|--------|-------|----------------|
| **RMSE** (Root Mean Squared Error) | **0.405** mg/m³ | Average deviation from ground-truth CO |
| **MAE** (Mean Absolute Error) | **0.256** mg/m³ | Average magnitude of absolute errors |
| **R² Score** (Coefficient of Determination) | **0.921** | 92.1% of CO variance is explained by the model |

### 2. Classification Metrics (Rubric Requirement)
*Obtained by grouping the continuous CO predictions into standard air quality levels.*

| Metric | Value | Note |
|--------|-------|------|
| **Accuracy** | **89.71%** | Percentage of binned predictions matching actual binned class |
| **F1 Score (weighted)** | **89.69%** | Balanced precision and recall across all classes |

The regressor is trained offline on 80% of the cleaned dataset (6,139 rows) and tested on the remaining 20% (1,535 rows). The continuous prediction is included in every live output message.

---

## Project Structure

```
Project - 1/
├── data/
│   └── AirQualityUCI.csv     # UCI Air Quality dataset (downloaded)
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

### Step 0 — Download the Dataset
Download the Air Quality dataset from https://archive.ics.uci.edu/dataset/360/air+quality and place the file `AirQualityUCI.csv` inside a `data/` folder in the project root. The expected path is `data/AirQualityUCI.csv`.

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

This trains the Random Forest Regressor on the Air Quality dataset and saves `model.joblib`.



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
Sent row    0 | Date: 10/03/2004 02.00.00 | CO: 2.60 mg/m³ | Temp: 13.6C
Sent row    1 | Date: 10/03/2004 03.00.00 | CO: 2.00 mg/m³ | Temp: 13.3C
```

**Consumer (Terminal 2):**
```
------------------------------------------------------------
[   1] Row 0  10/03/2004 02.00.00
  CO Measured  : 2.60 mg/m³
  CO Predicted : 2.48 mg/m³
  Error        : 0.12 mg/m³
  Conditions   : Temp 13.6C  |  Humidity 48.9%
```

---

## Dependencies

See requirements.txt for the full list of dependencies.

 
