FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Install against Airflow's own constraints file so shared deps (e.g. SQLAlchemy)
# stay at the versions Airflow 2.9.3's ORM models actually expect. Installing
# requirements.txt unconstrained pulls in SQLAlchemy>=2.0, which breaks
# `airflow db migrate` (Airflow 2.9's models aren't Mapped[]-annotated for SQLAlchemy 2.x).
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
