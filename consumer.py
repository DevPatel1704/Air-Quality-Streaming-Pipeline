# -*- coding: utf-8 -*-
"""
Reads prediction results from 'air-quality-predictions' and prints them.
Run with: python consumer.py
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
CYAN   = "\033[96m"
GREY   = "\033[90m"


def format_prediction(msg: dict, count: int) -> str:
    """Format a prediction message for human-readable console output."""
    row_id    = msg.get("row_id", "?")
    ts        = msg.get("timestamp", "")
    co_meas   = msg.get("CO_measured")
    predicted = msg.get("CO_predicted")
    temp      = msg.get("temperature_C")
    humidity  = msg.get("humidity_pct")

    co_str   = f"{co_meas:.2f} mg/m³" if co_meas is not None else "N/A"
    pred_str = f"{predicted:.2f} mg/m³" if predicted is not None else "N/A"
    
    err_str = "N/A"
    if co_meas is not None and predicted is not None:
        err = abs(co_meas - predicted)
        err_str = f"{err:.2f} mg/m³"

    temp_str = f"{temp:.1f}C" if temp is not None else "N/A"
    hum_str  = f"{humidity:.1f}%" if humidity is not None else "N/A"

    lines = [
        f"{CYAN}{'-'*60}{RESET}",
        f"{BOLD}[{count:>4}] Row {row_id}  {GREY}{ts}{RESET}",
        f"  CO Measured  : {co_str}",
        f"  CO Predicted : {pred_str}",
        f"  Error        : {err_str}",
        f"  Conditions   : Temp {temp_str}  |  Humidity {hum_str}",
    ]
    return "\n".join(lines)


def main():
    print(f"{BOLD}{CYAN}{'='*60}")
    print("    Air Quality CO Level  Live Predictions Consumer")
    print(f"{'='*60}{RESET}")
    print(f"Listening on topic: {BOLD}{PREDICTIONS_TOPIC}{RESET}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    total_error = 0.0
    try:
        for message in consumer:
            data = message.value
            if isinstance(data, (bytes, bytearray)):
                data = json.loads(data)

            count += 1
            
            meas = data.get("CO_measured")
            pred = data.get("CO_predicted")
            if meas is not None and pred is not None:
                total_error += abs(meas - pred)

            print(format_prediction(data, count))

            # Running MAE
            if count % 10 == 0:
                running_mae = total_error / count
                print(
                    f"\n{BOLD}   Running MAE: {running_mae:.2f} mg/m³ {RESET}\n"
                )

    except KeyboardInterrupt:
        final_mae = (total_error / count) if count > 0 else 0.0
        print(f"\n{CYAN}{'='*60}{RESET}")
        print(f"{BOLD}Consumer stopped.{RESET}")
        print(f"   Total predictions received : {count}")
        print(f"   Average MAE                : {final_mae:.2f} mg/m³")
        print(f"{CYAN}{'='*60}{RESET}")
        sys.exit(0)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

 
