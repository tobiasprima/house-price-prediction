import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
from utils import load_config, get_engine
import os


def main():
    config = load_config()
    engine = get_engine(config["database"]["url"])

    # Load data
    df = pd.read_sql("SELECT * FROM house_prices_marts.clean_house_prices", engine)
    y = df["saleprice"]
    X = df.drop(columns=["saleprice", "id"])
    X = pd.get_dummies(X, drop_first=True)

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config["training"]["test_size"],
        random_state=config["training"]["random_state"]
    )

    # XGBoost model
    model = XGBRegressor(**config["model"]["hyperparameters"])
    model.fit(X_train, y_train)

    # Metrics
    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)
    print(f"XGBoost → RMSE: {rmse:.2f}, R²: {r2:.3f}")

    # Save if enabled
    if config["training"].get("save_model", False):
        version = config["training"].get("model_version", 1)
        os.makedirs("models", exist_ok=True)
        model_path = f"models/house_price_xgboost_v{version}.pkl"
        joblib.dump(model, model_path)
        print(f"Saved model → {model_path}")

if __name__ == "__main__":
    main()
