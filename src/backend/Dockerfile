# Use a lightweight Python image
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy requirements from the parent directory (assuming it's in src)
COPY ../requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all backend files into /app
COPY . /app

# Set the command to run main.py from /app
ENTRYPOINT ["python", "/app/main.py"]
