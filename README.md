# Spark Integration Test

This project provides a test framework for running integration tests between Snowflake and Apache Spark using ClickBench data.

## Overview

The test framework consists of two main scripts:
- `make.sh` - Core utilities for Snowflake setup and data operations
- `clickbench.sh` - ClickBench dataset management and benchmarking

## Prerequisites

- Docker and Docker Compose
- Python with virtual environment support
- Snowflake CLI

## Quick Start

1. Start Docker services:
   ```bash
   sh make.sh up
   ```

2. Initialize Snowflake setup:
   ```bash
   sh make.sh setup
   ```

3. Load ClickBench data:
   ```bash
   sh clickbench.sh clickbench_partitioned
   ```

## Core Commands

### make.sh Commands

- `sh make.sh install_snowflake` - Install Snowflake CLI
- `sh make.sh up` - Start Docker Compose services
- `sh make.sh down` - Stop Docker Compose services
- `sh make.sh volume` - Create S3-based external volume
- `sh make.sh volume_file` - Create file-based external volume
- `sh make.sh database` - Create demo database
- `sh make.sh schema` - Create schema
- `sh make.sh setup` - Run complete Snowflake setup
- `sh make.sh snow_sql "query"` - Execute Snowflake SQL
- `sh make.sh spark_sql "query"` - Execute Spark SQL
- `sh make.sh equality table1 table2` - Compare data between tables

### clickbench.sh Commands

- `sh clickbench.sh cp_download_partitioned` - Download partitioned ClickBench data
- `sh clickbench.sh cb_download_single` - Download single ClickBench file
- `sh clickbench.sh cb_create_table` - Create ClickBench table schema
- `sh clickbench.sh cb_copy_into_partitioned` - Load partitioned data
- `sh clickbench.sh cb_copy_into_single` - Load single file data
- `sh clickbench.sh clickbench_partitioned` - Full partitioned setup
- `sh clickbench.sh clickbench_single` - Full single file setup
- `sh clickbench.sh clickbench_spark_partitioned` - Create Spark Iceberg table
- `sh clickbench.sh benchmark` - Run ClickBench queries and measure performance

## Creating Test Files

Test files should follow the pattern in `tests/example.sh`:

```bash
#!/bin/bash

# Start services
sh make.sh up

# Initialize Snowflake
sh make.sh setup

# Load test data
sh clickbench.sh clickbench_partitioned
sh clickbench.sh clickbench_spark_partitioned

# Run test queries
sh make.sh snow_sql "SELECT watchid FROM demo.spark.hits LIMIT 100;"
sh make.sh spark_sql "SELECT watchid FROM demo.embucket.hits LIMIT 100;"

# Verify data equality
sh make.sh equality demo.embucket.hits demo.spark.hits

# Cleanup
sh make.sh down
```

### Test File Structure

1. **Start services** - Use `sh make.sh up` to start Docker containers
2. **Initialize** - Run `sh make.sh setup` to create Snowflake resources
3. **Load data** - Choose appropriate data loading function
4. **Execute tests** - Run your specific test queries
5. **Verify results** - Use `sh make.sh equality` or custom validation
6. **Cleanup** - Use `sh make.sh down` to stop services

### Available Data Loading Options

- `sh clickbench.sh clickbench_partitioned` - Load all 100 partitioned files
- `sh clickbench.sh clickbench_partitioned_small` - Load only first partition for testing
- `sh clickbench.sh clickbench_single` - Load single large file
- `sh clickbench.sh clickbench_spark_partitioned` - Create corresponding Spark tables

## Storage Configuration

Two storage types are configured:
- **S3 storage** (`mybucket`) - MinIO-based object storage
- **File storage** (`local`) - Local filesystem access

Both point to the same data location for testing different ingestion paths.

## Usage Examples

### Basic Integration Test
```bash
sh tests/example.sh
```

### Custom Test Creation
```bash
# Create new test file
cp tests/example.sh tests/my_test.sh
# Edit to add your specific test logic
# Run your test
sh tests/my_test.sh
```

### Manual Operations
```bash
# Start only the infrastructure
sh make.sh up
sh make.sh setup

# Load specific dataset
sh clickbench.sh clickbench_single

# Run custom queries
sh make.sh snow_sql "SELECT COUNT(*) FROM demo.embucket.hits"
sh make.sh spark_sql "SELECT COUNT(*) FROM demo.spark.hits"

# Compare results
sh make.sh equality demo.embucket.hits demo.spark.hits

# Cleanup
sh make.sh down
```

## Environment Variables

The scripts automatically handle:
- `SNOWFLAKE_HOME` - Set to current project directory
- Virtual environment activation via `venv.sh`