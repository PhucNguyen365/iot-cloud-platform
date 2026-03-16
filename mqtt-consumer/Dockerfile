FROM python:3.12-slim
WORKDIR /app
COPY app.py .
RUN pip install paho-mqtt mysql-connector-python
CMD ["python", "-u", "app.py"]