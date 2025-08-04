# Base image
FROM docker.io/jaihind213/spark-py:3.5.2-scala2.13-java11-ubuntu

# Version arguments
ARG spark_uid=185

# Set up environment
USER root
# You will notice the name '/opt/data_pipeline_app'. kept it generic for reusing the same code infra elsewhere.
# dont worry, i shall copy the readme into the image. so that you can idenitfy the project.
# After all, its for demo purposes.
WORKDIR /opt/data_pipeline_app

# Create required directories
RUN mkdir -p /opt/data_pipeline_app/config \
    && mkdir -p /opt/data_pipeline_app/data \
    && mkdir -p /opt/data_pipeline_app/etl \
    && mkdir -p /opt/spark_jars \
    && chmod -R 755 /opt/spark_jars \

COPY ./README.md /opt/data_pipeline_app/
COPY ./requirements.txt /opt/data_pipeline_app/etl
COPY ./spark_jars /opt/spark_jars/

# Copy project files
COPY etl/*.py /opt/data_pipeline_app/etl/

# Install Python dependencies
RUN python3 -m pip config set global.break-system-packages true \
 && pip3 install --no-cache-dir -r /opt/data_pipeline_app/etl/requirements.txt

# Set permissions
RUN chown -R ${spark_uid}:${spark_uid} /opt/data_pipeline_app \
 && chmod -R 777 /opt/data_pipeline_app/config \
 && chmod -R 777 /opt/data_pipeline_app/data \
 && chown -R ${spark_uid}:${spark_uid} /opt/spark_jars && chmod 777 /opt/spark_jars/*

# Environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Default command
CMD ["python3"]

# Drop to non-root user
USER ${spark_uid}