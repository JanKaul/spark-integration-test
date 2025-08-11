#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    spark = SparkSession.builder \
        .appName("CreateIcebergTable") \
        .getOrCreate()
    
    return spark

def load_parquet_and_create_iceberg_table(spark, parquet_path="/home/iceberg/partitioned", table_name="demo.spark.hits"):
    """Load parquet files and create Iceberg table"""
    
    # Read all parquet files from the partitioned folder
    print(f"Loading parquet files from: {parquet_path}")
    df = spark.read.parquet(f"{parquet_path}/hits_0.parquet")
    
    print(f"Schema of loaded data:")
    df.printSchema()
    
    print(f"Total rows: {df.count()}")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS demo.spark;")

    # Create Iceberg table
    print(f"Creating Iceberg table: {table_name}")
    df.writeTo(table_name).createOrReplace()
    
    print(f"Successfully created Iceberg table: {table_name}")
    
    # Verify the table was created
    result = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
    result.show()

def main():
    spark = create_spark_session()
    
    try:
        load_parquet_and_create_iceberg_table(spark)
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
