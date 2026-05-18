# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
Trains a Random Forest Regressor to predict the actual CO concentration in mg/m³.
Run with: python train_model.py
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

DATA_FILE = "data/AirQualityUCI.csv"
MODEL_FILE = "model.joblib"

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


def train(df: pd.DataFrame):
    """Train and evaluate the Random Forest Regressor."""
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
    y = df["CO(GT)"].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nTraining Random Forest Regressor, this might take a sec...")
    print(f"   Train size: {len(X_train)} | Test size: {len(X_test)}")

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # --- Evaluation ---
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\n{'='*50}")
    print(f"MODEL PERFORMANCE")
    print(f"{'='*50}")
    print(f"  RMSE      : {rmse:.4f}")
    print(f"  MAE       : {mae:.4f}")
    print(f"  R2 Score  : {r2:.4f}")

    # --- Save ---
    payload = {
        "model": model,
        "features": available,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
    }
    joblib.dump(payload, MODEL_FILE)
    print(f"Saved to {MODEL_FILE}")
    print(f"{'='*50}\n")

    return rmse, mae, r2


if __name__ == "__main__":
    df = load_and_clean(DATA_FILE)
    rmse, mae, r2 = train(df)
    print("All done. Training complete! You can now run the pipeline.")
    print(f"   RMSE: {rmse:.4f}  |  MAE: {mae:.4f}  |  R2: {r2:.4f}")

