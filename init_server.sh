#!/bin/bash

# Tạo trước các thư mục để tránh Docker tự tạo bằng quyền root
mkdir -p volumes/nodered volumes/mosquitto/data volumes/mosquitto/log volumes/mysql_data

# Cấp quyền cho từng dịch vụ
chown -R 1000:1000 volumes/nodered/
chown -R 1883:1883 volumes/mosquitto/
chown -R 999:999 volumes/mysql_data/

# Xử lý quyền Let's Encrypt (Bỏ qua lỗi nếu chưa cài certbot)
chmod -R 755 /etc/letsencrypt/live/ 2>/dev/null
chmod -R 755 /etc/letsencrypt/archive/ 2>/dev/null