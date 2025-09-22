export SNOWFLAKE_HOME=$(pwd)

source ./venv.sh

if [ ! -d "tpch" ]; then
  mkdir tpch
  mkdir -p tpch/10
fi

volume_local_file() {
  snow sql -q "CREATE EXTERNAL VOLUME 'local'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'local'
      STORAGE_PROVIDER = 'file'
      STORAGE_BASE_URL = '$(pwd)/tpch'
    )
  )"
}

setup() {
  volume_local
}

tpch_create_table() {
  snow sql -q "CREATE TABLE demo.embucket.customer (
            C_CUSTKEY BIGINT NOT NULL,
            C_NAME VARCHAR NOT NULL,
            C_ADDRESS VARCHAR NOT NULL,
            C_NATIONKEY BIGINT NOT NULL,
            C_PHONE VARCHAR NOT NULL,
            C_ACCTBAL DOUBLE NOT NULL,
            C_MKTSEGMENT VARCHAR NOT NULL,
            C_COMMNET VARCHAR NOT NULL);"
}

cb_copy_into_partitioned_small() {
  cb_copy_into_partitioned_n 0
}

cb_copy_into_partitioned_n() {
  local n=$1
  snow sql -q "COPY INTO demo.embucket.hits FROM 'file:///storage/clickbench/partitioned/hits_$n.parquet' STORAGE_INTEGRATION = local FILE_FORMAT = (TYPE = PARQUET);"
}

cb_copy_into_partitioned() {
  snow sql -q "COPY INTO demo.embucket.hits FROM 'file:///storage/clickbench/partitioned/' STORAGE_INTEGRATION = local FILE_FORMAT = (TYPE = PARQUET);"
}

tpch_copy_into_customer_file() {
  snow sql -q "COPY INTO demo.embucket.customer FROM 'file://$(pwd)/tpch/10/customer.csv' STORAGE_INTEGRATION = local FILE_FORMAT = (TYPE = CSV);"
}

tpch_customer() {
  tpch_create_table
  tpch_copy_into_customer_file
}

clickbench_spark() {
  docker exec spark-iceberg spark-submit /home/iceberg/create_iceberg.py
}

clickbench_spark_partitioned() {
  docker exec spark-iceberg spark-submit /home/iceberg/create_iceberg_partitioned.py
}

benchmark() {
  echo "query_number,execution_time_seconds" >clickbench/results.csv
  query_num=1
  cat clickbench/queries.sql | while read -r query; do
    if [[ -n "$query" && ! "$query" =~ ^[[:space:]]*$ ]]; then
      start_time=$(date +%s.%N)
      snow sql -q "$query"
      end_time=$(date +%s.%N)
      execution_time=$(awk "BEGIN {print $end_time - $start_time}")
      echo "$query_num,$execution_time" >>clickbench/results.csv
      query_num=$((query_num + 1))
    fi
  done
}

activate

if [ -n "$1" ]; then "$1" "${2:0}"; fi
