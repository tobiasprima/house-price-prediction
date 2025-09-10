import pandas as pd
import joblib
import json
from utils import load_config

# Load trained model
config = load_config()
version = config ["training"].get("model_version")
model_path = f"models/house_price_xgboost_v{version}.pkl"
model = joblib.load(model_path)
print(f"Loaded model from {model_path}")

# Example input
sample = {
    "grlivarea": 2000,
    "totalbaths": 2.5,
    "houseage": 20,
    "remodage": 10,
    "totrmsabvgrd": 7,
    "bedroom": 3,
    "garagecars": 2,
    "garagearea": 500,
    "totalporchsf": 120,
    "miscval": 0,
    "mosold": 6,
    "yrsold": 2010,
    "overallqual": 7,
    "overallcond": 5,
    "lotarea": 8000,
    "totalbsmtsf": 900,
    "firstflrsf": 1200,
    "secondflrsf": 800,
    "kitchenqual_enc": 4,
    "functional_enc": 5,
    "fireplacequ_enc": 3,
    "garagefinish_enc": 2,
    "garagequal_enc": 3,
    "garagecond_enc": 3,
    "paveddrive_enc": 2,
    "poolqc_enc": 0,
    "garagetype": "Attchd",
    "saletype": "WD",
    "salecondition": "Normal",
    "neighborhood": "CollgCr",
    "centralair": "Y"
}

# Convert to DataFrame
X_new = pd.DataFrame([sample])

# One-hot encode categoricals
X_new = pd.get_dummies(X_new)

# Align columns with training model
trained_columns = model.get_booster().feature_names 
for col in trained_columns:
    if col not in X_new.columns:
        X_new[col] = 0
X_new = X_new[trained_columns]

# Predict
pred = model.predict(X_new)[0]
print(f"Predicted Sale Price: ${pred:,.0f}")
