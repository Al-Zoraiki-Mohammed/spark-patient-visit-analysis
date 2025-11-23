# Spark Patient Visit Analysis
* A PySpark-based data processing pipeline that analyzes patient visit records to determine whether each patient meets specific consecutive monthly visit conditions within the last year of the observation period.

## Project Purpose
* Healthcare organizations often need to identify patients who consistently visit clinics over time — for compliance, treatment monitoring, or eligibility checks.
This PySpark job processes a raw dataset of patient visits and answers the key question:
“Did each patient have enough consecutive monthly visits in the last 12 months before the cutoff date?”
* Specifically, the job determines for each patient whether they achieved:
    * 5 consecutive monthly visits
    * 9 consecutive monthly visits
    * 11 consecutive monthly visits
These results are output as boolean flags in a final CSV file.

## How the Logic Works (Inference From Script)
* The script performs the following steps:
1. Load patient visit data
Each record contains:
effective_from_date — the visit date (MMDDYYYY format)
patient_id
2. Convert the visit date to a real date type
3. Filter visits to the last 365 days
Everything outside the 1-year window preceding the cutoff date (2016-09-30) is ignored.
4. Calculate time between consecutive visits
For each patient, visits are ordered chronologically, and Spark computes:
    * days_since_last_visit
5. Determine consecutive monthly visits
A new column classifies whether each visit is consecutive (within ~1 month).
A running sum is generated to compute streak length.
6. Evaluate conditions
    - For each patient
        * 5months	Did the patient have ≥5  consecutive qualified visits?
        * 9months	Did the patient have ≥9 consecutive visits?
        * 11months	Did the patient reach 11 consecutive visits?
7. Output results
    -Spark writes results to:
        * output/result.csv/
        * Containing: patient_id,5months,9months,11months.
## Running the Project with Docker
1. Build and run: 'docker compose up --build'
2. After completion: Output is generated in: output/result.csv/. 
    - This folder contains:
        * part-00000...csv — actual result
        * _SUCCESS — Spark job success flag

## Docker Components
1. Dockerfile: Uses python:3.10-slim
    * Installs Java via default-jdk (compatible with ARM64 / M1 / M2 Macs). 
    * Installs PySpark
    * Sets up runtime environment.
2. docker-compose.yml
    * Mounts local input/, output/, and src/ into the container.
    * Runs the PySpark job once and exits (batch job).

## Extending the Pipeline
* You may extend the logic to:
    1. Compute additional visit streak thresholds.
    2. Handle multiple years of data.
    3. Export results to Parquet or a data warehouse.
    4. Run in a Spark cluster instead of standalone.