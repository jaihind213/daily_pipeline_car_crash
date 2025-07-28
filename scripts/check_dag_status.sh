#!/usr/bin/env bash

set -e

DAG_NAME=$1
RUN_ID=$2
USER=$3
PASSWD=$4
AIRFLOW_HOST=$5
AIRFLOW_PORT=8080

touch /tmp/dag_status_${RUN_ID}
curl  -o /tmp/dag_status_${RUN_ID} -X GET "http://${AIRFLOW_HOST}:8080/api/v1/dags/${DAG_NAME}/dagRuns/${RUN_ID}" \
-H "Content-Type: application/json" \
--user "${USER}:${PASSWD}"

cat /tmp/dag_status_${RUN_ID}
cat /tmp/dag_status_${RUN_ID}| grep -i '"state": "success"' || exit 1