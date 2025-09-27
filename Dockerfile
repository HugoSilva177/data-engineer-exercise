# Use official Python image
FROM python:3.11-slim

# Create directories
RUN mkdir -p /app/duckdb
RUN mkdir -p /app/duckdb/exports
RUN mkdir -p /app/dbt_project

# Copy files
COPY ./dbt_project /app/dbt_project
COPY ./src /app
COPY ./requirements.txt /app

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

WORKDIR /app/dbt_project

RUN dbt deps --project-dir /app/dbt_project