# Use a lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only frontend code (assuming 'frontend/' contains app.py and related files)
COPY frontend/ ./frontend

# Expose Streamlit's default port
EXPOSE 8501


# Default command (can be overridden in docker-compose)
CMD ["streamlit", "run", "frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
