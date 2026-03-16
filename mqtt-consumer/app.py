import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import os
from datetime import datetime


print("Waiting for MySQL to start...")
time.sleep(15) 

db = None
while db is None:
    try:
        db = mysql.connector.connect(
            host="mysql",       
            user="root",
            password=os.environ.get("MYSQL_ROOT_PASSWORD"),
            database="iot_db"
        )
        print("Successfully connected to MySQL!")
    except Exception as e:
        print(f"MySQL not ready, retrying in 5s...")
        time.sleep(5)

cursor = db.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS sensor_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    temperature FLOAT NOT NULL,
    humidity FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
db.commit()
print("Table 'sensor_data' is ready!")


MQTT_BROKER = "broker.hivemq.com"  
MQTT_PORT = 1883

MQTT_TOPIC = "phucops/sensor/+"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker! Subscribing to topic: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
       
        payload = json.loads(msg.payload.decode("utf-8"))
        
        device_id = payload.get("device_id")
        temp = payload.get("temperature")
        hum = payload.get("humidity")

        
        if device_id and temp is not None and hum is not None:
            sql = "INSERT INTO sensor_data (device_id, temperature, humidity) VALUES (%s, %s, %s)"
            val = (device_id, temp, hum)
            cursor.execute(sql, val)
            db.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved: {device_id} | Temp: {temp}°C | Hum: {hum}%")
        else:
            print("Invalid payload format!")

    except Exception as e:
        print(f"Error processing message: {e}")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

connected = False
while not connected:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        connected = True
    except Exception as e:
        print(f"MQTT Broker not ready, retrying in 5s...")
        time.sleep(5)

print("MQTT Consumer is running and listening for data...")
client.loop_forever()