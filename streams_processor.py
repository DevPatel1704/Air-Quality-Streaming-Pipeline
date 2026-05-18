"""
Consumes raw air quality events from 'air-quality-raw', runs the Random Forest Regressor to predict CO levels, and publishes numeric results to 'air-quality-predictions'.

Run with: python streams_processor.py worker -l info
"""

# -*- coding: utf-8 -*-

import faust_patch
faust_patch.apply()

import json
import warnings
import joblib
import numpy as np
import pandas as pd
import faust
from config import MODEL_FILE, RAW_TOPIC, PREDICTIONS_TOPIC, FAUST_BROKER, get_faust_credentials

warnings.filterwarnings("ignore", category=UserWarning)

# --- Load model at startup ---
print(f"Loading model from {MODEL_FILE}...")
_payload  = joblib.load(MODEL_FILE)
MODEL     = _payload["model"]
FEATURES  = _payload["features"]
print(f"Model ready. Features used: {len(FEATURES)}")

# --- Faust App ---
app = faust.App(
    "air-quality-processor",
    broker=FAUST_BROKER,
    broker_credentials=get_faust_credentials(),
    value_serializer="json",
    topic_allow_declare=False,       # Topics already exist on Confluent Cloud
    topic_disable_leader=True,       # Skip leader-related admin calls
)

# --- Topic Definitions ---
raw_topic         = app.topic(RAW_TOPIC,         value_type=bytes)
predictions_topic = app.topic(PREDICTIONS_TOPIC, value_type=bytes)


def extract_features(event: dict) -> pd.DataFrame | None:
    """Extract feature DataFrame (with column names) from raw event dict."""
    try:
        row = {}
        for feat in FEATURES:
            val = event.get(feat)
            if val is None or val != val:
                val = 0.0
            row[feat] = float(val)
        return pd.DataFrame([row], columns=FEATURES)
    except Exception as e:
        print(f"Skipping row, couldn't extract features: {e}")
        return None


# --- Faust Agent (Streams Processor) ---
@app.agent(raw_topic, sink=[predictions_topic])
async def process_air_quality(stream):
    """
    Faust agent that:
      1. Receives raw sensor messages from 'air-quality-raw'
      2. Extracts ML features
      3. Runs the Random Forest Regressor
      4. Yields numeric prediction results to 'air-quality-predictions'
    """
    async for event in stream:
        # Deserialize if bytes
        if isinstance(event, (bytes, bytearray)):
            event = json.loads(event)

        row_id   = event.get("row_id", "?")
        co_actual = event.get("CO(GT)", None)

        # Extract features and predict
        X = extract_features(event)
        if X is None:
            continue

        predicted_co = float(round(MODEL.predict(X)[0], 3))

        # Build prediction output message
        result = {
            "row_id":          row_id,
            "timestamp":       f"{event.get('Date', '')} {event.get('Time', '')}".strip(),
            "CO_measured":     round(float(co_actual), 3) if co_actual is not None else None,
            "CO_predicted":    predicted_co,
            "temperature_C":   event.get("T"),
            "humidity_pct":    event.get("RH"),
            "features_used":   len(FEATURES),
        }

        yield json.dumps(result).encode("utf-8")


if __name__ == "__main__":
    app.main()

 
