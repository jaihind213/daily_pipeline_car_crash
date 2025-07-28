# Base image
FROM docker.io/jaihind213/spark-py:3.5.2-scala2.13-java11-ubuntu

# Version arguments
ARG spark_uid=185

# Set up environment
USER root
WORKDIR /opt/daily_pipeline_car_crash/car_crash

# Create required directories
RUN mkdir -p /opt/daily_pipeline_car_crash/config \
    && mkdir -p /opt/daily_pipeline_car_crash/data \
    && mkdir -p /opt/daily_pipeline_car_crash/car_crash \
    && mkdir -p /opt/spark_jars \
    && chmod -R 755 /opt/spark_jars

COPY ./requirements.txt /opt/daily_pipeline_car_crash/car_crash
COPY ./spark_jars /opt/spark_jars/

# Copy project files
COPY car_crash/*.py /opt/daily_pipeline_car_crash/car_crash

# Install Python dependencies
RUN python3 -m pip config set global.break-system-packages true \
 && pip3 install --no-cache-dir -r /opt/daily_pipeline_car_crash/car_crash/requirements.txt

# Set permissions
RUN chown -R ${spark_uid}:${spark_uid} /opt/daily_pipeline_car_crash \
 && chmod -R 777 /opt/daily_pipeline_car_crash/config \
 && chmod -R 777 /opt/daily_pipeline_car_crash/data \
 && chown -R ${spark_uid}:${spark_uid} /opt/spark_jars && chmod 777 /opt/spark_jars/*

# Environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Default command
CMD ["python3"]

# Drop to non-root user
USER ${spark_uid}