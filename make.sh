export SNOWFLAKE_HOME=$(pwd)

venv() {
  if [ ! -d "venv" ]; then python -m venv venv; fi
}

activate() {
  venv
  if [ -z "$VIRTUAL_ENV" ]; then . venv/bin/activate; fi
}

install_snowflake() {
  pip install snowflake-cli
}

docker_up() {
  docker compose up -d
}

docker_down() {
  docker compose down -v
}

volume() {
  snow sql -q "CREATE EXTERNAL VOLUME 'mybucket'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'mybucket'
      STORAGE_PROVIDER = 's3'
      STORAGE_BASE_URL = 'mybucket'
      STORAGE_ENDPOINT = 'http://warehouse.minio:9000'
      CREDENTIALS = (
        AWS_KEY_ID = 'AKIAIOSFODNN7EXAMPLE'
        AWS_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        REGION = 'us-east-2'
      )
    )
  )"
}

volume_file() {
  snow sql -q "CREATE EXTERNAL VOLUME 'local'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'local'
      STORAGE_PROVIDER = 'file'
      STORAGE_BASE_URL = '/storage'
    )
  )"
}

database() {
  snow sql -q "CREATE DATABASE demo EXTERNAL_VOLUME = 'mybucket';"
}

schema() {
  snow sql -q "CREATE SCHEMA demo.embucket;"
}

setup() {
  volume
  volume_file
  database
  schema
}

snow_sql() {
  snow sql -q "$1"
}

spark_sql() {
  docker exec spark-iceberg spark-sql -e "$1"
}

equality() {
  spark_query "select sum(hash(*)) from $1"
  spark_query "select sum(hash(*)) from $2"
}

activate

if [ -n "$1" ]; then "$1" "${2:0}"; fi
