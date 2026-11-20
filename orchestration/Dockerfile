# 1. Base Image: Start with the official Airflow image (Python 3.11)
FROM apache/airflow:2.10.3-python3.11

# 2. Switch to ROOT user so we have permission to install system packages (Java)
USER root

# 3. Update Linux package manager and install OpenJDK 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. Set the JAVA_HOME environment variable so PySpark knows where Java is
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 5. Switch back to the non-root 'airflow' user (standard Docker security best practice)
USER airflow

# 6. Install our project Python libraries inside the container (including awscli for s3 sync)
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    httpx==0.27.0 \
    kafka-python-ng==2.2.3 \
    boto3==1.34.131 \
    awscli

# 7. Copy project DAGs and code into the image (so Docker needs no external host mounts)
COPY --chown=airflow:root orchestration/dags/ /opt/airflow/dags/
COPY --chown=airflow:root transformation/ /opt/airflow/transformation/