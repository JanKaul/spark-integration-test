export SNOWFLAKE_HOME=$(pwd)

source ./venv.sh

install_snowflake() {
  pip install snowflake-cli
}

up() {
  docker compose up -d
}

down() {
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

volume_local() {
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

volume_file() {
  snow sql -q "CREATE EXTERNAL VOLUME 'file'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'file'
      STORAGE_PROVIDER = 'file'
      STORAGE_BASE_URL = '$(pwd)/storage'
    )
  )"
}

volume_file_clickbench() {
  snow sql -q "CREATE EXTERNAL VOLUME 'local'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'local'
      STORAGE_PROVIDER = 'file'
      STORAGE_BASE_URL = '$(pwd)/clickbench'
    )
  )"
}

database_file() {
  snow sql -q "CREATE DATABASE demo EXTERNAL_VOLUME = 'file';"
}

schema() {
  snow sql -q "CREATE SCHEMA demo.embucket;"
}

setup() {
  volume
  volume_local
  database
  schema
}

setup_file() {
  volume_file
  volume_file_clickbench
  database_file
  schema
}

snowsql() {
  snow sql -q "$1"
}

sparksql() {
  docker exec spark-iceberg spark-sql -e "$1"
}

equality() {
  spark_sql "select sum(hash(*)) from $1"
  spark_sql "select sum(hash(*)) from $2"
}

activate

if [ -n "$1" ]; then "$1" "${2:0}"; fi
