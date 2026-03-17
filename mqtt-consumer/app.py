import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import os
from datetime import datetime

# Khởi tạo kết nối DB với cơ chế Reconnect
print("Waiting for MySQL to start...")
time.sleep(15) 

db = None
while db is None:
    try:
        db = mysql.connector.connect(
            host="mysql",       
            user="root",
            password=os.environ.get("MYSQL_ROOT_PASSWORD"), # Rút mật khẩu từ biến môi trường Docker
            database="iot_db"
        )
        print("Successfully connected to MySQL!")
    except Exception as e:
        print(f"MySQL not ready, retrying in 5s...")
        time.sleep(5)

cursor = db.cursor()

# Migration: Tự động khởi tạo cấu trúc bảng lưu trữ nếu chưa tồn tại
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

# Cấu hình kết nối tới MQTT Broker nội bộ
MQTT_BROKER = "mqtt" 
MQTT_PORT = 1883  
MQTT_TOPIC = "sensor/+"

# Callback: Kích hoạt khi thiết lập kết nối thành công tới Broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to Internal MQTT Broker! Subscribing to topic: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC) # Đăng ký lắng nghe Topic
    else:
        print(f"Failed to connect, return code {rc}")

# Callback: Kích hoạt khi có luồng Message mới đẩy vào Topic
def on_message(client, userdata, msg):
    try:
        # --- THÊM ĐOẠN NÀY ĐỂ FIX LỖI LOST CONNECTION ---
        global db, cursor
        try:
            db.ping(reconnect=True, attempts=3, delay=1)
        except:
            # Nếu ping thất bại hoàn toàn, cố gắng tạo lại connection mới
            db = mysql.connector.connect(
                host="mysql",       
                user="root",
                password=os.environ.get("MYSQL_ROOT_PASSWORD"),
                database="iot_db"
            )
            cursor = db.cursor()
        # ------------------------------------------------

        # Giải mã chuỗi JSON từ Payload của thiết bị
        payload = json.loads(msg.payload.decode("utf-8"))
        
        device_id = payload.get("device_id")
        temp = payload.get("temperature")
        hum = payload.get("humidity")

        # Validate dữ liệu cơ bản trước khi Insert vào DB
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

# Khởi tạo MQTT Client và mapping các hàm Callback
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Giữ kết nối MQTT sống sót, thử lại nếu Broker chưa up
connected = False
while not connected:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        connected = True
    except Exception as e:
        print(f"Internal MQTT Broker not ready, retrying in 5s...")
        time.sleep(5)

print("MQTT Consumer is running and listening for data...")

# Block thread chính, chạy vòng lặp vô hạn để duy trì việc nhận dữ liệu
client.loop_forever()