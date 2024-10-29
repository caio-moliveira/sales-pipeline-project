# Use a lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements from project root
COPY ../requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main script and all source files from src into /app
COPY ../ /app

# Set the command to run main.py
ENTRYPOINT ["python", "main.py"]
