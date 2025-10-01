source ./make.sh
source ./clickbench.sh

snowsql "CREATE EXTERNAL VOLUME 'bucket'
  STORAGE_LOCATIONS = 
  (
    (
      NAME = 'bucket'
      STORAGE_PROVIDER = 's3'
      STORAGE_BASE_URL = 'mybucket'
      STORAGE_ENDPOINT = 'http://127.0.0.1:9000'
      CREDENTIALS = (
        AWS_KEY_ID = 'AKIAIOSFODNN7EXAMPLE'
        AWS_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        REGION = 'us-east-2'
      )
    )
  )"

volume_local_file
database
schema

cb_create_table

for i in {0..19}; do cb_copy_into_partitioned_n_file $i; done
