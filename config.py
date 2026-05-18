"""
config.py - Kafka & App Configuration
Confluent Cloud setup for ENGR 5785G Assignment 1.
"""

import os
import ssl
import faust

# --- Confluent Cloud Credentials ---
_BROKER  = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
_API_KEY = "WTSUASSMYC4CWLXE"
_API_SECRET = "cfltEvilrvrmvCJYZ9A38PehtShO8A+6rfuebmSWd3xFyDmHUgVFQTU8eEPjkScw"

# Allow overrides via environment variables
KAFKA_BROKER     = os.getenv("KAFKA_BROKER",     _BROKER)
KAFKA_API_KEY    = os.getenv("KAFKA_API_KEY",    _API_KEY)
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET", _API_SECRET)

# --- Topic Names ---
RAW_TOPIC         = "air-quality-raw"
PREDICTIONS_TOPIC = "air-quality-predictions"

# --- Producer Settings ---
PRODUCER_DELAY = 1.0          # seconds between each row sent
DATA_FILE      = "data/AirQualityUCI.csv"
MODEL_FILE     = "model.joblib"

# --- Build kafka-python producer/consumer config ---
def get_kafka_config() -> dict:
    """Returns kafka-python compatible config dict."""
    return {
        "bootstrap_servers": KAFKA_BROKER,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism":    "PLAIN",
        "sasl_plain_username": KAFKA_API_KEY,
        "sasl_plain_password": KAFKA_API_SECRET,
    }

# --- Faust Broker URL & Credentials ---
FAUST_BROKER = f"kafka://{KAFKA_BROKER}"

def get_faust_credentials() -> faust.SASLCredentials:
    """Returns Faust SASLCredentials for Confluent Cloud SASL_SSL."""
    return faust.SASLCredentials(
        username=KAFKA_API_KEY,
        password=KAFKA_API_SECRET,
        ssl_context=ssl.create_default_context(),
        mechanism="PLAIN",
    )
