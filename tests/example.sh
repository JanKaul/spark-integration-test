source ./clickbench.sh

docker_up
setup

cb_create_table
cb_copy_into_n 0

cb_spark_create_table

snow_sql "select watchid from demo.spark.hits limit 100;"
spark_sql "select watchid from demo.embucket.hits limit 100;"

equality demo.embucket.hits demo.spark.hits

docker_down
