#!/bin/bash

#we have a single secret in github_secrets in the form of a json.
#ex:
#{ "SOCRATA_TOKEN": "user1", "S3_ACCESS_KEY": "pass1", "S3_SECRET_KEY": "abc123", "ICEBERG_PATH": "", "RAW_DATA_PATH": "" }
# this has been done to avoid multiple exports in the github workflow file.
JSONFILE=$1
KEY_FILE=$2
touch $KEY_FILE
chmod 400 $KEY_FILE
for key in $(jq -r 'keys[]' $JSONFILE); do
  value=$(jq -r --arg k "$key" '.[$k]'  $JSONFILE)
  echo "$key=$value" >> $GITHUB_ENV
  echo "$key=$value" >> $KEY_FILE
done