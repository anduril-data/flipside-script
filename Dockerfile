# Use an official Python runtime as the base image
FROM python:3.8-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "main.py"]
