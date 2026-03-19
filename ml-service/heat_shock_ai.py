import mysql.connector
import pandas as pd
import joblib
import time
import os
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split

# Khởi tạo kết nối DB, có cơ chế retry chờ container mysql khởi động xong
print("Waiting for MySQL to start...")
time.sleep(15)

db = None
while db is None:
    try:
        # Lấy mật khẩu an toàn từ biến môi trường của Docker Compose
        db = mysql.connector.connect(
            host="mysql", user="root", password=os.environ.get("MYSQL_ROOT_PASSWORD"), database="iot_db"
        )
        print("Successfully connected to MySQL!")
    except Exception as e:
        print(f"MySQL connection failed: {e}. Retrying in 5s...")
        time.sleep(5)

# Hàm tính toán chỉ số nhiệt (Heat Index) theo công thức chuẩn của NOAA
def heat_index(T, H):
    return (-8.784695 + 1.61139411*T + 2.338549*H - 0.14611605*T*H -
            0.012308094*T*T - 0.016424828*H*H + 0.002211732*T*T*H +
            0.00072546*T*H*H - 0.000003582*T*T*H*H)

# ==========================================
# KHỐI HUẤN LUYỆN MÔ HÌNH AI (Chỉ chạy 1 lần khi khởi động)
# ==========================================
try:
    query = "SELECT device_id, temperature, humidity, created_at FROM sensor_data ORDER BY created_at"
    df = pd.read_sql(query, db)
    
    # Yêu cầu tối thiểu 10 dòng dữ liệu để bắt đầu train
    if len(df) >= 10:
        df = df.dropna(subset=["device_id"])
        
        # Phân tách dữ liệu theo luồng cảm biến trong nhà và ngoài trời
        room = df[df["device_id"] == "ESP32_PhongKhachin"].reset_index(drop=True)
        outdoor = df[df["device_id"] == "ESP32_PhongKhachout"].reset_index(drop=True)
        
        # Cân bằng độ dài 2 tập dữ liệu để ghép DataFrame
        min_len = min(len(room), len(outdoor))
        
        data = pd.DataFrame({
            "indoor_temp": room["temperature"][:min_len],
            "outdoor_temp": outdoor["temperature"][:min_len],
            "indoor_humidity": room["humidity"][:min_len],
            "outdoor_humidity": outdoor["humidity"][:min_len]
        })

        # Feature Engineering: Thêm đặc trưng chênh lệch nhiệt và Heat Index
        data["delta_temp"] = abs(data["outdoor_temp"] - data["indoor_temp"])
        data["heat_index"] = data.apply(lambda x: heat_index(x["outdoor_temp"], x["outdoor_humidity"]), axis=1)
        
        # Gán nhãn (Labeling): 1 (Nguy cơ) nếu chênh lệch > 10 độ HOẶC Heat index > 40
        data["label"] = data.apply(lambda x: 1 if x["delta_temp"] > 10 or x["heat_index"] > 40 else 0, axis=1)
        
        # Shift dữ liệu để mô hình học cách dự đoán cho thời điểm tiếp theo
        data["future_label"] = data["label"].shift(-1)
        data = data.dropna()

        # Phân chia tập Train/Test
        X = data[["indoor_temp", "outdoor_temp", "delta_temp", "heat_index"]]
        y = data["future_label"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Huấn luyện mô hình Decision Tree
        model = DecisionTreeClassifier()
        model.fit(X_train, y_train)
        print("Model accuracy:", model.score(X_test, y_test))
        
        # Export mô hình ra file để tái sử dụng ở luồng Monitor
        joblib.dump(model, "heat_model.pkl")
    else:
        print("Not enough data to train AI yet.")
except Exception as e:
    print(f"Training skipped: {e}")


# ==========================================
# KHỐI GIÁM SÁT VÀ DỰ ĐOÁN THỜI GIAN THỰC
# ==========================================
print("Start AI monitoring...")
if os.path.exists("heat_model.pkl"):
    model = joblib.load("heat_model.pkl")
    cursor = db.cursor()
    
    while True:
        try:
            # Truy vấn 20 bản ghi mới nhất để lấy trạng thái môi trường hiện tại
            cursor.execute("SELECT device_id, temperature, humidity FROM sensor_data ORDER BY created_at DESC LIMIT 20")
            rows = cursor.fetchall()
            indoor = outdoor = indoor_h = outdoor_h = None
            
            # Lọc ra thông số mới nhất của từng khu vực
            for device, temp, hum in rows:
                if device == "ESP32_PhongKhachin" and indoor is None: indoor, indoor_h = temp, hum
                if device == "ESP32_PhongKhachout" and outdoor is None: outdoor, outdoor_h = temp, hum

            # Tiến hành dự đoán nếu thu thập đủ dữ liệu từ 2 trạm
            if indoor is not None and outdoor is not None:
                delta = abs(outdoor - indoor)
                hi = heat_index(outdoor, outdoor_h)
                
                # Nạp thông số vào AI để xuất kết quả dự đoán
                pred = model.predict([[indoor, outdoor, delta, hi]])
                print(f"Indoor: {indoor} | Outdoor: {outdoor} | Risk: {'YES' if pred[0]==1 else 'NO'}")
        except Exception as e:
            pass
        
        # Chu kỳ quét dữ liệu mới: 5s/lần
        time.sleep(5)
else:
    print("AI Model not found. Standing by...")
    while True: time.sleep(100)