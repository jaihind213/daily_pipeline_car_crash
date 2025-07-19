#!/usr/bin/env bash
DAG_NAME=$1
DAG_DATE=$2
USER=$3
PASSWD=$4
AIRFLOW_HOST=$5

AIRFLOW_PORT=8080

EPOCH_TIME=$(date +%s)
RUN_ID="dag_runid_${EPOCH_TIME}"

if [ -z "$DAG_DATE" ]; then
    echo "Error: Date parameter is required (format: yyyy-mm-dd)"
    echo "Usage: $0 DAG_NAME DAG_DATE USER PASSWD AIRFLOW_HOST"
    exit 1
fi

curl -X PATCH  "http://${AIRFLOW_HOST}:8080/api/v1/dags/$DAG_NAME" \
-H "Content-Type: application/json" \
--user "${USER}:${PASSWD}" \
-d '{"is_paused": false}'

echo "unpausing dag... $DAG_NAME"
echo "triggering dag... $DAG_NAME with RUNID=$RUN_ID, DATE=$DAG_DATE"
sleep 10
curl -X POST "http://${AIRFLOW_HOST}:8080/api/v1/dags/${DAG_NAME}/dagRuns" \
-H "Content-Type: application/json" \
--user "${USER}:${PASSWD}" \
-d "{
  \"dag_run_id\": \"${RUN_ID}\",
  \"date\": \"${DAG_DATE}\"
  \"note\": \"string\"
}"

touch /tmp/dag_run_id
echo $RUN_ID >> /tmp/dag_run_id