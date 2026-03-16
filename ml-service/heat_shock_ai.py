import mysql.connector
import pandas as pd
import joblib
import time
import os
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split

print("Waiting for MySQL to start...")
time.sleep(15)

db = None
while db is None:
    try:
        db = mysql.connector.connect(
            host="mysql", user="root", password="123456", database="iot_db"
        )
        print("Successfully connected to MySQL!")
    except Exception as e:
        print(f"MySQL not ready yet, retrying in 5s...")
        time.sleep(5)

def heat_index(T, H):
    return (-8.784695 + 1.61139411*T + 2.338549*H - 0.14611605*T*H -
            0.012308094*T*T - 0.016424828*H*H + 0.002211732*T*T*H +
            0.00072546*T*H*H - 0.000003582*T*T*H*H)

try:
    query = "SELECT device_id, temperature, humidity, created_at FROM sensor_data ORDER BY created_at"
    df = pd.read_sql(query, db)
    
    if len(df) >= 10:
        df = df.dropna(subset=["device_id"])
        room = df[df["device_id"] == "esp32_room1"].reset_index(drop=True)
        outdoor = df[df["device_id"] == "esp32_room2"].reset_index(drop=True)
        min_len = min(len(room), len(outdoor))
        
        data = pd.DataFrame({
            "indoor_temp": room["temperature"][:min_len],
            "outdoor_temp": outdoor["temperature"][:min_len],
            "indoor_humidity": room["humidity"][:min_len],
            "outdoor_humidity": outdoor["humidity"][:min_len]
        })

        data["delta_temp"] = abs(data["outdoor_temp"] - data["indoor_temp"])
        data["heat_index"] = data.apply(lambda x: heat_index(x["outdoor_temp"], x["outdoor_humidity"]), axis=1)
        data["label"] = data.apply(lambda x: 1 if x["delta_temp"] > 10 or x["heat_index"] > 40 else 0, axis=1)
        data["future_label"] = data["label"].shift(-1)
        data = data.dropna()

        X = data[["indoor_temp", "outdoor_temp", "delta_temp", "heat_index"]]
        y = data["future_label"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = DecisionTreeClassifier()
        model.fit(X_train, y_train)
        print("Model accuracy:", model.score(X_test, y_test))
        joblib.dump(model, "heat_model.pkl")
    else:
        print("Not enough data to train AI yet.")
except Exception as e:
    print(f"Training skipped: {e}")

print("Start AI monitoring...")
if os.path.exists("heat_model.pkl"):
    model = joblib.load("heat_model.pkl")
    cursor = db.cursor()
    while True:
        try:
            cursor.execute("SELECT device_id, temperature, humidity FROM sensor_data ORDER BY created_at DESC LIMIT 20")
            rows = cursor.fetchall()
            indoor = outdoor = indoor_h = outdoor_h = None
            for device, temp, hum in rows:
                if device == "esp32_room1" and indoor is None: indoor, indoor_h = temp, hum
                if device == "esp32_room2" and outdoor is None: outdoor, outdoor_h = temp, hum

            if indoor is not None and outdoor is not None:
                delta = abs(outdoor - indoor)
                hi = heat_index(outdoor, outdoor_h)
                pred = model.predict([[indoor, outdoor, delta, hi]])
                print(f"Indoor: {indoor} | Outdoor: {outdoor} | Risk: {'YES' if pred[0]==1 else 'NO'}")
        except Exception as e:
            pass
        time.sleep(5)
else:
    print("AI Model not found. Standing by...")
    while True: time.sleep(100)