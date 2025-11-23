#!/bin/bash

echo "Running PySpark Job..."

# Execute your PySpark script
python3 /app/src/spark_task.py

echo "Job Completed. Output saved in ./output/"