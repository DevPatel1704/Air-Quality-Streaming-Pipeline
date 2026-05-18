# -*- coding: utf-8 -*-
"""
producer.py - Kafka Producer
Reads rows from the Air Quality UCI dataset and publishes each row
as a JSON message to the 'air-quality-raw' Kafka topic at ~1 row/second.

Usage:
    python producer.py
"""

import json
import time
import numpy as np
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import DATA_FILE, PRODUCER_DELAY, RAW_TOPIC, get_kafka_config

CO_BINS   = [0, 1.5, 4.0, 100]
CO_LABELS = ["Low", "Medium", "High"]

FEATURE_COLS = [
    "PT08.S1(CO)",
    "C6H6(GT)",
    "PT08.S2(NMHC)",
    "NOx(GT)",
    "PT08.S3(NOx)",
    "NO2(GT)",
    "PT08.S4(NO2)",
    "PT08.S5(O3)",
    "T",
    "RH",
    "AH",
]


def load_data(path: str) -> pd.DataFrame:
    """Load and clean the Air Quality dataset."""
    df = pd.read_csv(path, sep=";", decimal=",")
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    df.replace(-200, float("nan"), inplace=True)
    df.replace(-200.0, float("nan"), inplace=True)
    df.dropna(subset=["CO(GT)"], inplace=True)
    df = df[df["CO(GT)"] > 0]
    df.fillna(df.median(numeric_only=True), inplace=True)

    # Add CO_Level ground truth label for reference
    df["CO_Level_actual"] = pd.cut(
        df["CO(GT)"],
        bins=CO_BINS,
        labels=CO_LABELS,
        right=True
    ).astype(str)

    df.reset_index(drop=True, inplace=True)
    return df


def row_to_event(row: pd.Series, row_num: int) -> dict:
    """Convert a DataFrame row to a Kafka event dict."""
    event = {"row_id": row_num}
    for col in ["Date", "Time", "CO(GT)", "CO_Level_actual"]:
        if col in row.index:
            val = row[col]
            event[col] = None if pd.isna(val) else val

    for col in FEATURE_COLS:
        if col in row.index:
            val = row[col]
            event[col] = None if (pd.isna(val) or val != val) else float(val)

    return event


def main():
    print("=" * 55)
    print("  Air Quality Kafka Producer")
    print("=" * 55)

    # Load dataset
    df = load_data(DATA_FILE)
    print(f" Loaded {len(df)} rows from {DATA_FILE}")
    print(f" Broker  : {get_kafka_config()['bootstrap_servers']}")
    print(f" Topic   : {RAW_TOPIC}")
    print(f"  Delay   : {PRODUCER_DELAY}s per row")
    print("-" * 55)

    # Create producer
    kafka_cfg = get_kafka_config()
    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        **{k: v for k, v in kafka_cfg.items() if k != "bootstrap_servers"},
    )

    print(" Connected to Kafka broker. Starting stream...\n")

    sent = 0
    try:
        for idx, row in df.iterrows():
            event = row_to_event(row, idx)
            future = producer.send(RAW_TOPIC, value=event)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"  Failed to send row {idx}: {e}")
                continue

            sent += 1
            co_val  = event.get("CO(GT)", "?")
            co_lvl  = event.get("CO_Level_actual", "?")
            temp    = event.get("T", "?")
            dt      = event.get("Date", "")
            tm      = event.get("Time", "")

            print(
                f"[{sent:>4}]  Sent row {idx:>4} | "
                f"Date: {dt} {tm} | CO: {co_val:.2f} mg/m | "
                f"Temp: {temp:.1f}C | Level: {co_lvl}"
            )
            time.sleep(PRODUCER_DELAY)

    except KeyboardInterrupt:
        print(f"\n Producer stopped by user. Sent {sent} messages.")
    finally:
        producer.flush()
        producer.close()
        print(f" Producer closed. Total sent: {sent}")


if __name__ == "__main__":
    main()
