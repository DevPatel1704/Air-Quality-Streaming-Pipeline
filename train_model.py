# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
train_model.py - Offline ML Model Training
Trains a Random Forest Classifier to predict CO concentration level
(Low / Medium / High) from air quality sensor readings.

Run this ONCE before starting the pipeline:
    python train_model.py
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.preprocessing import LabelEncoder

DATA_FILE = "data/AirQualityUCI.csv"
MODEL_FILE = "model.joblib"

# --- CO Level Bins ---
# CO(GT) is true hourly averaged CO concentration in mg/m³
# We classify into 3 levels based on typical urban air quality thresholds
CO_BINS   = [0, 1.5, 4.0, 100]
CO_LABELS = ["Low", "Medium", "High"]

def load_and_clean(path: str) -> pd.DataFrame:
    """Load the UCI Air Quality CSV and clean it."""
    print(f"Loading dataset from {path}...")
    # The file uses semicolons as delimiters and commas for decimals (European format)
    df = pd.read_csv(path, sep=";", decimal=",", parse_dates=False)

    # Drop unnamed trailing columns (artifact of the Excel export)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    # Drop Date and Time columns (not needed for ML features)
    df.drop(columns=["Date", "Time"], inplace=True, errors="ignore")

    # Replace -200 sentinel (missing value marker) with NaN
    df.replace(-200, np.nan, inplace=True)
    df.replace(-200.0, np.nan, inplace=True)

    # Drop rows where target (CO(GT)) is missing
    df.dropna(subset=["CO(GT)"], inplace=True)

    # Drop rows where CO(GT) <= 0 (invalid readings)
    df = df[df["CO(GT)"] > 0]

    # Fill remaining NaN with column medians
    df.fillna(df.median(numeric_only=True), inplace=True)

    print(f"Cleaned dataset: {len(df)} rows, {df.shape[1]} columns")
    return df


def create_target(df: pd.DataFrame) -> pd.DataFrame:
    """Bin CO(GT) into Low / Medium / High categories."""
    df = df.copy()
    df["CO_Level"] = pd.cut(
        df["CO(GT)"],
        bins=CO_BINS,
        labels=CO_LABELS,
        right=True
    )
    df.dropna(subset=["CO_Level"], inplace=True)
    print(f"\nCO Level distribution:\n{df['CO_Level'].value_counts().to_string()}")
    return df


def train(df: pd.DataFrame):
    """Train and evaluate the Random Forest Classifier."""
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

    # Keep only columns that exist in the dataset
    available = [c for c in FEATURE_COLS if c in df.columns]
    print(f"\nUsing {len(available)} features: {available}")

    X = df[available]
    y = df["CO_Level"].astype(str)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\nTraining Random Forest Classifier...")
    print(f"   Train size: {len(X_train)} | Test size: {len(X_test)}")

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # --- Evaluation ---
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1  = f1_score(y_test, y_pred, average="weighted")

    print(f"\n{'='*50}")
    print(f"MODEL PERFORMANCE")
    print(f"{'='*50}")
    print(f"  Accuracy  : {acc:.4f} ({acc*100:.2f}%)")
    print(f"  F1 Score  : {f1:.4f} (weighted)")
    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=CO_LABELS))

    # --- Save ---
    payload = {
        "model": model,
        "features": available,
        "co_bins": CO_BINS,
        "co_labels": CO_LABELS,
        "accuracy": acc,
        "f1_score": f1,
    }
    joblib.dump(payload, MODEL_FILE)
    print(f"Model saved to {MODEL_FILE}")
    print(f"{'='*50}\n")

    return acc, f1


if __name__ == "__main__":
    df = load_and_clean(DATA_FILE)
    df = create_target(df)
    acc, f1 = train(df)
    print("Done! Training complete! You can now run the pipeline.")
    print(f"   Accuracy: {acc*100:.2f}%  |  F1: {f1:.4f}")
