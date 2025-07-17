#!/usr/bin/env bash
DAG_NAME=$1
USER=$2
PASSWD=$3
AIRFLOW_HOST=$4

AIRFLOW_PORT=8080

EPOCH_TIME=$(date +%s)
RUN_ID="angiogram_runid_${EPOCH_TIME}"

curl -X PATCH  "http://${AIRFLOW_HOST}:8080/api/v1/dags/$DAG_NAME" \
-H "Content-Type: application/json" \
--user "${USER}:${PASSWD}" \
-d '{"is_paused": false}'

echo "unpausing dag... $DAG_NAME"
echo "triggering dag... $DAG_NAME with RUNID=$RUN_ID"
sleep 10
curl -X POST "http://${AIRFLOW_HOST}:8080/api/v1/dags/${DAG_NAME}/dagRuns" \
-H "Content-Type: application/json" \
--user "${USER}:${PASSWD}" \
-d "{
  \"dag_run_id\": \"${RUN_ID}\",
  \"conf\": {},
  \"note\": \"string\"
}"

touch /tmp/angiogram_dag_run_id
echo $RUN_ID >> /tmp/angiogram_dag_run_id