import pandas as pd
import glob
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import joblib

# ── 1. Load all CSVs ──────────────────────────────────────────
files = glob.glob(os.path.join("data", "*.csv"))
df = pd.concat([pd.read_csv(f, encoding='latin-1') for f in files], ignore_index=True)

# ── 2. Rename messy columns ───────────────────────────────────
df.rename(columns={
    'ï»¿"Longitude (x)"'      : 'Longitude',
    'Temp (Â°C)'               : 'Temp',
    'Dew Point Temp (Â°C)'     : 'DewPoint',
    'Rel Hum (%)'              : 'Humidity',
    'Wind Spd (km/h)'          : 'WindSpd',
    'Stn Press (kPa)'          : 'Pressure',
    'Visibility (km)'          : 'Visibility',
}, inplace=True)

# ── 3. Keep only useful columns ───────────────────────────────
features = ['Temp', 'DewPoint', 'Humidity', 'WindSpd', 'Pressure', 'Visibility']
df = df[features].copy()

# ── 4. Drop rows with missing values ─────────────────────────
df.dropna(inplace=True)
print(f"Clean rows available: {len(df)}")

# ── 5. Create target — next hour temperature ─────────────────
df['Next_Temp'] = df['Temp'].shift(-1)
df.dropna(inplace=True)

# ── 6. Split features and target ─────────────────────────────
X = df[features]
y = df['Next_Temp']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ── 7. Train the model ────────────────────────────────────────
print("Training model...")
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# ── 8. Evaluate ───────────────────────────────────────────────
y_pred = model.predict(X_test)
mae   = mean_absolute_error(y_test, y_pred)
r2    = r2_score(y_test, y_pred)

print(f"\n✅ Model trained successfully!")
print(f"   Mean Absolute Error : {mae:.2f} °C")
print(f"   R² Score            : {r2:.4f}")

# ── 9. Save the model ─────────────────────────────────────────
os.makedirs("model", exist_ok=True)
joblib.dump(model, "model/weather_model.pkl")
print(f"\n✅ Model saved to model/weather_model.pkl")

# ── 10. Save cleaned data for producer later ─────────────────
df.to_csv("data/clean_weather.csv", index=False)
print(f"✅ Clean dataset saved to data/clean_weather.csv")