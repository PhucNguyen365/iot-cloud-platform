FROM python:3.12-slim
WORKDIR /app
COPY heat_shock_ai.py .
RUN pip install pandas scikit-learn joblib mysql-connector-python requests
CMD ["python", "-u", "heat_shock_ai.py"]