# Use the official Airflow image
FROM apache/airflow:2.6.3-python3.10

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Copy the DAGs, data, and scripts
COPY dags/ $AIRFLOW_HOME/dags/
COPY data/ $AIRFLOW_HOME/data/
COPY scripts/ $AIRFLOW_HOME/scripts/

# Initialize the database and set up Airflow commands
RUN airflow db init
