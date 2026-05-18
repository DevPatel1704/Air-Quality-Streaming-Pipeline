# -*- coding: utf-8 -*-
"""
consumer.py - Output Consumer
==============================
Reads prediction results from the 'air-quality-predictions' topic
and prints each result to the console in a readable, colour-coded format.

Usage:
    python consumer.py
"""

import json
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import PREDICTIONS_TOPIC, get_kafka_config

# ANSI colour codes for terminal output
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
GREY   = "\033[90m"

LEVEL_COLORS = {
    "Low":    GREEN,
    "Medium": YELLOW,
    "High":   RED,
}

def colour(text: str, level: str) -> str:
    c = LEVEL_COLORS.get(level, RESET)
    return f"{BOLD}{c}{text}{RESET}"

def format_prediction(msg: dict, count: int) -> str:
    """Format a prediction message for human-readable console output."""
    row_id    = msg.get("row_id", "?")
    ts        = msg.get("timestamp", "")
    co_meas   = msg.get("CO_measured")
    actual    = msg.get("CO_actual_label", "?")
    predicted = msg.get("CO_predicted", "?")
    conf      = msg.get("confidence_pct", 0)
    correct   = msg.get("correct", None)
    temp      = msg.get("temperature_C")
    humidity  = msg.get("humidity_pct")

    co_str   = f"{co_meas:.2f} mg/m" if co_meas is not None else "N/A"
    temp_str = f"{temp:.1f}C" if temp is not None else "N/A"
    hum_str  = f"{humidity:.1f}%" if humidity is not None else "N/A"

    tick = f"{GREEN}{RESET}" if correct else f"{RED}{RESET}"
    pred_coloured   = colour(predicted, predicted)
    actual_coloured = colour(actual, actual)

    lines = [
        f"{CYAN}{''*60}{RESET}",
        f"{BOLD}[{count:>4}] Row {row_id}  {GREY}{ts}{RESET}",
        f"  CO Measured : {co_str}",
        f"  Predicted   : {pred_coloured}  (confidence: {conf:.1f}%)",
        f"  Actual      : {actual_coloured}  {tick}",
        f"  Conditions  : Temp {temp_str}  |  Humidity {hum_str}",
    ]
    return "\n".join(lines)


def main():
    print(f"{BOLD}{CYAN}{'='*60}")
    print("    Air Quality CO Level  Live Predictions Consumer")
    print(f"{'='*60}{RESET}")
    print(f" Listening on topic: {BOLD}{PREDICTIONS_TOPIC}{RESET}")
    print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{GREY}(Press Ctrl+C to stop){RESET}\n")

    kafka_cfg = get_kafka_config()
    consumer = KafkaConsumer(
        PREDICTIONS_TOPIC,
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="air-quality-consumer-group",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        **{k: v for k, v in kafka_cfg.items() if k != "bootstrap_servers"},
    )

    count = 0
    correct_count = 0
    try:
        for message in consumer:
            data = message.value
            if isinstance(data, (bytes, bytearray)):
                data = json.loads(data)

            count += 1
            if data.get("correct"):
                correct_count += 1

            print(format_prediction(data, count))

            # Running accuracy
            if count % 10 == 0:
                running_acc = (correct_count / count) * 100
                print(
                    f"\n{BOLD}   Running Accuracy: {running_acc:.1f}% "
                    f"({correct_count}/{count} correct){RESET}\n"
                )

    except KeyboardInterrupt:
        final_acc = (correct_count / count * 100) if count > 0 else 0
        print(f"\n{CYAN}{'='*60}{RESET}")
        print(f"{BOLD} Consumer stopped.{RESET}")
        print(f"   Total predictions received : {count}")
        print(f"   Correct predictions        : {correct_count}")
        print(f"   Final Accuracy             : {final_acc:.2f}%")
        print(f"{CYAN}{'='*60}{RESET}")
        sys.exit(0)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
