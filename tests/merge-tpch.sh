source ./make.sh
source ./tpch.sh

setup_file
volume_local

tpch_create_table

snowsql "CREATE TABLE demo.embucket.source (
            C_CUSTKEY BIGINT NOT NULL,
            C_NAME VARCHAR NOT NULL,
            C_ADDRESS VARCHAR NOT NULL,
            C_NATIONKEY BIGINT NOT NULL,
            C_PHONE VARCHAR NOT NULL,
            C_ACCTBAL DOUBLE NOT NULL,
            C_MKTSEGMENT VARCHAR NOT NULL,
            C_COMMNET VARCHAR NOT NULL);"

snowsql "COPY INTO demo.embucket.source FROM 'file://$(pwd)/tpch/10/customer.csv' STORAGE_INTEGRATION = local FILE_FORMAT = (TYPE = CSV);"

sleep 5

snowsql "MERGE INTO demo.embucket.customer target USING demo.embucket.source source ON target.C_CUSTKEY = source.C_CUSTKEY WHEN MATCHED THEN UPDATE SET C_NAME = source.C_NAME, C_ADDRESS = source.C_ADDRESS, C_NATIONKEY = source.C_NATIONKEY, C_PHONE = source.C_PHONE, C_ACCTBAL = source.C_ACCTBAL, C_MKTSEGMENT = source.C_MKTSEGMENT, C_COMMNET = source.C_COMMNET WHEN NOT MATCHED THEN INSERT (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMNET) VALUES (source.C_CUSTKEY, source.C_NAME, source.C_ADDRESS, source.C_NATIONKEY, source.C_PHONE, source.C_ACCTBAL, source.C_MKTSEGMENT, source.C_COMMNET);"
