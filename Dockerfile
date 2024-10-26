# Use an official Python image as the base
FROM python:3.12-slim

# Set environment variables to prevent bytecode generation and enable buffer output
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set the working directory
WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy the Poetry files and install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy the entire application code
COPY . .

# Define the default command to run main.py (ETL process)
CMD ["poetry", "run", "python", "main.py"]