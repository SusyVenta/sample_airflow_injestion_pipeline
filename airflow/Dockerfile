# =============================================================================
# airflow/Dockerfile
# Custom Airflow image that adds:
#   - OpenJDK 17  (required by spark-submit / PySpark)
#   - PySpark 3.5.3
#   - apache-airflow-providers-apache-spark  (SparkSubmitOperator)
#   - apache-airflow-providers-postgres      (PostgresOperator)
#   - psycopg2-binary                        (Airflow metadata DB driver)
# =============================================================================

FROM apache/airflow:2.9.3

# Switch to root to install system packages
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Create an arch-independent symlink so JAVA_HOME works on both amd64 and arm64
    && ln -sf "$(ls -d /usr/lib/jvm/java-17-openjdk-* | head -1)" /usr/local/java17

# Use the stable symlink as JAVA_HOME (resolves correctly on any architecture)
ENV JAVA_HOME=/usr/local/java17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to the airflow user for pip installs
USER airflow

# Pin versions for reproducibility
RUN pip install --no-cache-dir \
    "pyspark==3.5.3" \
    "apache-airflow-providers-apache-spark==4.10.0" \
    "apache-airflow-providers-postgres==5.11.1" \
    "psycopg2-binary==2.9.9" \
    "pytest==8.3.4" \
    "pytest-cov==6.0.0"
