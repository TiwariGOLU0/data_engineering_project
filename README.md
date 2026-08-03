# DE_Project
This project implements an end-to-end ETL pipeline that reads data from AWS S3 and a relational database, applies transformations to build customer and sales data marts with proper dimension table joins, and writes the processed data back to S3 and the database in Parquet format. The pipeline supports environment-specific configurations for DEV, QA, and PROD, following enterprise-style deployment practices, and includes utility modules for logging, encryption/decryption, and Spark session management to keep the codebase production-aware.

## Skills Used in This Project:
- Python
- SQL
- PySpark
- AWS
- ETL/ELT


## Project Structure

```text
Project Structure:-

my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │   ├── config.py
│   │   └── requirement.txt
│   ├── qa/
│   │   ├── config.py
│   │   └── requirement.txt
│   ├── prod/
│   │   ├── config.py
│   │   └── requirement.txt
│   └── sql_scripts/
│       └── table_scripts.sql
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   ├── delete/
│   │   │   ├── aws_delete.py
│   │   │   ├── database_delete.py
│   │   │   └── local_file_delete.py
│   │   ├── download/
│   │   │   └── aws_file_download.py
│   │   ├── move/
│   │   │   └── move_files.py
│   │   ├── read/
│   │   │   ├── aws_read.py
│   │   │   └── database_read.py
│   │   ├── transformations/
│   │   │   └── jobs/
│   │   │       ├── customer_mart_sql_transform_write.py
│   │   │       ├── dimension_tables_join.py
│   │   │       ├── main.py
│   │   │       └── sales_mart_sql_transform_write.py
│   │   ├── upload/
│   │   │   └── upload_to_s3.py
│   │   ├── utility/
│   │   │   ├── encrypt_decrypt.py
│   │   │   ├── logging_config.py
│   │   │   ├── s3_client_object.py
│   │   │   ├── spark_session.py
│   │   │   └── my_sql_session.py
│   │   └── write/
│   │       ├── database_write.py
│   │       └── parquet_write.py
│   └── test/
│       ├── scratch_pad.py.py
│       └── generate_csv_data.py

```


