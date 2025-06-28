FROM docker.io/jaihind213/spark-py:3.5.2-scala2.13-java11-ubuntu

ARG spark_uid=185
USER root
RUN mkdir -p /opt/daily_pipeline_car_crash/config
RUN mkdir -p /opt/daily_pipeline_car_crash/data
WORKDIR /opt/daily_pipeline_car_crash
COPY . /opt/daily_pipeline_car_crash

RUN python3 -m pip config set global.break-system-packages true
RUN pip3 install -r /opt/daily_pipeline_car_crash/requirements.txt

RUN chown -R 185:185 /opt/daily_pipeline_car_crash
RUN chmod -R 777 /opt/daily_pipeline_car_crash/config
RUN chmod -R 777 /opt/daily_pipeline_car_crash/data

CMD ["python3"]
USER ${spark_uid}