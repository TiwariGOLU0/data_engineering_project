import os.path
import shutil
from datetime import datetime
import mysql
from pyspark.examples.src.main.python.als import update
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from resources.dev.config import product_staging_table
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.read import database_read
from src.main.read.aws_read import *
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.move.move_files import *
from src.main.write import parquet_writer
from src.main.write.parquet_writer import ParquetWriter
from src.test.sales_data_upload_s3 import s3_directory

############# Get S3 Client #############
aws_access_key = config.aws_access_key
aws_secret_key =config.aws_secret_key



s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client =s3_client_provider.get_client()

#Now use S3_client for s3 operations
response = s3_client.list_buckets()
# print(response)
logger.info("List of Buckets: %s" , response['Buckets'])

#check if local directory has already a file
#if file is there then check if the same file is present in the staging area
#with status as A .if so then don't delete and try to re-run
#Else give an error and not process the next file
csv_files =[file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
connection = get_mysql_connection()
cursor =connection.cursor()

total_csv_files =[]
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    files_string = ", ".join([f"'{f}'" for f in total_csv_files])

    statement = f"""
    select distinct file_name from
    de_project.product_staging_table
    where file_name in ({str(total_csv_files)[1:-1]})
    and status = "A"
    """


    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("no csv files found")

else:
    logger.info("Your last run was successful please check")


try:
    s3_reader = S3Reader()
    #Bucket name should come from table
    folder_path =config.s3_source_directory
    s3_absolute_file_path =s3_reader.list_files(
        s3_client,
        config.bucket_name,
        folder_path=folder_path)

    logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No files available at {folder_path}")

except Exception as e:
    logger.error("Exited with error: %s",e)
    raise e


bucket_name =config.bucket_name
local_directory =config.local_directory

prefix =f"s3://{bucket_name}/"
file_path =[url[len(prefix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name",bucket_name)
logging.info(f"File path available on s3 under{bucket_name } bucket and folder name is {file_path}")
try:
    downloader =S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_path)
except Exception as e:
    logger.error("File download error: %s",e)
    sys.exit()

#Get a list of all files in the local directory
all_files =os.listdir(local_directory)
logger.info(f"List of files present in my local directory after downloading: {all_files}")

# Get a list of all files in the local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present in my local directory after downloading: {all_files}")

# Filter files with ".csv" in their name and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file)))

    if not csv_files:
        logger.info("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
    else:
        logger.info(f"CSV files ready for processing: {csv_files}")





logger.info("***********Listing the files************")
logger.info("List of csv files that needs to be processed %s ",csv_files)

logger.info("***************Creating Spark session*****************")


spark =spark_session()

logger.info("******************spark session created*******************")


#check the required column in the schema of csv files
#if not required colun keep it in list or error files.
#else union all the data into one dataframe

logger.info("****************checking schema for data loaded in s3******************")

correct_files=[]
for data in csv_files:
    data_schema=spark.read.format("csv")\
                .option("header", "true") .option("inferSchema", "true")\
                .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.mandatory_columns}")
    missing_columns=set(config.mandatory_columns)-set(data_schema)
    logger.info(f"missing column are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)


logger.info(f"**************List of currect files*******************{correct_files}")
logger.info(f"***********************List of error files******************************{error_files}")
logger.info("******************Moving Error data to error directory if any***********************")

#Move the data to error directory on local
error_folder_local_path =config.error_folder_path_local


def move_s2_to_s3(s3_client, source_prefix, destination_prefix):
    pass


if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name =os.path.basename(file_path)
            destination_path =os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"Moved '{file_name}' from s3 path to {destination_path}")


            source_prefix =config.s3_source_directory
            destination_prefix =config.s3_error_directory

            message =move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")

        else:
            logger.info(f"'{file_name}' does not exist")
else:
    logger.info("**************There is no error file available in our database*****************")



#Before running the process
#store table needs to be uploaded with statuse as Active(A) or inactive(I)
logger.info(f"**********updating the product_staging_table that we have started the process**********")
insert_statement= []
db_name =config.database_name
current_date =datetime.now()
formatted_date =current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename= os.path.basename(file)
        statement =f"""
INSERT INTO {db_name}.{config.product_staging_table} 
(file_name,file_location,created_date,status )
VALUES('{filename}','{filename}','{formatted_date}','A') """
        insert_statement.append(statement)
    logger.info(f"Insert statement created for staging table--{insert_statement}")
    logger.info("***********Connecting with My SQL Server*************")
    connection = get_mysql_connection()
    cursor = connection.cursor()

    logger.info("*********My sql Server connected successfully*************")
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
        cursor.close()
        connection.close()

else:
    logger.info("*************There is no file to process ************")
    raise Exception("*********No data Available with currect files**********")



logger.info("***********Staging table updates successfully************")

logger.info("******Fixing extra column coming from source *******")

schema =StructType(
    [
    StructField("customer_id",IntegerType(),True),
        StructField("store_id",IntegerType(),True),
        StructField("product_name",StringType(),True),
        StructField("sales_date",StringType(),True),
        StructField("sales_person_id",IntegerType(),True),
        StructField("price",FloatType(),True),
        StructField("quantity",IntegerType(),True),
        StructField("total_cost",FloatType(),True),
        StructField("additional_columns",StringType(),True)
    ]
)

#connecting with DatabaseReader
#database_client=DatabaseReader(config.url,config.properties)
logger.info("**********Creating empty dataframe************")
#final_df_to_process=database_client.create_dataframe(spark,"empty_df_create_table")
final_df_to_process=spark.createDataFrame([],schema=schema)


#create a new column with concatenated values of extra columns
for data in correct_files:
    data_df=spark.read.format("csv")\
            .option("header", "true") .option("inferSchema", "true") .load(data)
    data_schema=data_df.columns
    extra_column=list(set(data_schema)-set(config.mandatory_columns))
    logger.info(f"Extra column present at source is {extra_column}")
    if extra_column:
        data_df=data_df.withColumn("additional_column",concat_ws(",",*extra_column))\
                .select("customer_id","store_id","product_name","sales_date",
                        "sales_person_id","price","quantity","total_cost","additional_column")
        logger.info(f"processed {data} and added 'additional_column")
    else:
        data_df=data_df.withColumn("additional_column",lit(None))\
                .select("customer_id","store_id","product_name","sales_date",
                        "sales_person_id","price","quantity","total_cost","additional_column")

    final_df_to_process=final_df_to_process.union(data_df)
#final_df_to_process=data_df
logger.info("**********Final Dataframe from source which will be going to proccess************")
final_df_to_process.show()

#enrich the data from all dimension table
#also create a datamart for sales_team and their incentive address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that their should be a store_id segrigation

#read the data from parquet and generate a csv file in which there will be a sales_person_name ,sales_person_store_id
#sales_person_total_billing_done for each month, total_incentive
#
#
#connecting with DatabaseReader
database_client =DatabaseReader(config.url,config.properties)

#creating df for all tables
#customer table
logger.info("***********Loading customer table into customer_table_df*************")
customer_table_df=database_client.create_dataframe(spark,config.customer_table_name)
#product table
logger.info("*********Loading product table into product_table_df*************")
product_table_df=database_client.create_dataframe(spark,config.product_table)
logger.info("*********Loading stagging table into staging_table_df*************")
#product staging table
product_staging_df=database_client.create_dataframe(spark,config.product_staging_table)
logger.info("*********Loading sales table into sales_table_df*************")
#sales table
sales_team_table_df =database_client.create_dataframe(spark,config.sales_team_table)
#store table
logger.info("************Loading store table into store_table_df****************")
store_team_table_df =database_client.create_dataframe(spark,config.store_table)


s3_customer_store_sales_df_join=dimesions_table_join(
    final_df_to_process,
    customer_table_df,
    store_team_table_df,
    sales_team_table_df
)

#final enriched data
logger.info("************final Enriched data**************")
s3_customer_store_sales_df_join.show()



#write the customer data into customer data mart in parquet format
#file will be written to local first
#move the RAW data to s3 bucket for reporting tool
#write reporting data into MySQL TABLE ALDO
logger.info("*********write the data into Customer Data mart***********")
final_customer_data_mart_df=s3_customer_store_sales_df_join\
                .select("ct.customer_id",
                        "ct.first_name","ct.last_name","ct.address","ct.pincode",
                        "phone_number","sales_date","total_cost")
logger.info("*********Final data for customer data mart**********")
final_customer_data_mart_df.show()

parquet_writer =ParquetWriter("Overwrite","parquet")
parquet_writer.dataframe_write(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"*******customer data write to local disk at{config.customer_data_mart_local_file}********")

#move data on s3 bucket for customer_data_mart
logger.info("*****Data Movement from local to s3 for customer data mart******")
s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_sales_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")


#sales_team Data mart
logger.info("*****write the data into sales team data mart*****")
from pyspark.sql.functions import col, substring

final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select(
    "store_id",
    "sales_person_id",
    "sales_person_first_name",
    "sales_person_last_name",
    "store_manager_name",
    "manager_id",
    "is_manager",
    "sales_person_address",
    "sales_person_pincode",
    "sales_date",
    "total_cost",
    substring(col("sales_date"), 1, 7).alias("sales_month")
)

logger.info("********Final data for sales team data mart**********")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                 config.sales_team_data_mart_local_file)
logger.info(f"********sales team data written to local disk at {config.sales_team_data_mart_local_file}********")



#Move data on s3 bucket for sales_data_mart
s3_directory=config.s3_sales_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,
                                config.bucket_name,
                                 config.sales_team_data_mart_local_file)

logger.info(f"{message}")


#Also writing the data into partitions
final_sales_team_data_mart_df.format("parquet")\
                .option("header","true")\
                .mode("overwrite") \
                .partitionBy("sales_month","store_id")\
                .option("path",config.sales_team_data_mart_partitioned_local_file)\
                .save()

#Move data on s3 for partitioned folder
s3_prefix ="sales_partitioned_data_mart"
current_epoch=int(datetime.now().timestamp())*1000
for root ,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key= f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)


#calculation for customer mart
#find out the customer total purchase every mounth
#write the data into MySQL table
logger.info("*********calculating customer every month purchesed amount**********")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("********Calculation of customer mart done and write into the table **********")


#calculation for sales data mart
#find out the total sales done by each sales person every month
#Give the top performer 1% incentive of total sales of the month
#rest sales person will get nothing
#write the data into mysql table


logger.info("********Calculating sales every month billed amount**********")

sales_mart_calculation_table_write(final_sales_team_data_mart_df)

logger.info("*********Calculation of sales mart done and written into the table**********")


##################Last Step#####################
#move file on s3 into processed folder and delete the local files
source_prefix =config.s3_error_directory
destination_prefix =config.s3_error_directory
message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")


logger.info("********** Deleting sales data from local **********")
delete_local_file(config.local_directory)
logger.info("********** Deleted sales data from local **********")


logger.info("********** Deleting sales team data mart file from local **********")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("********** Deleted sales team data mart file from local **********")


logger.info("********** Deleting partitioned sales team data mart file from local **********")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********** Deleted partitioned sales team data mart file from local **********")

# update the status of staging table

update_statements = []

if correct_files:

    for file in files:
        filename = os.path.basename(file)

        statement = (
            f"UPDATE {db_name}.{config.product_staging_table} "
            f"SET status = 'IF', updated_date = '{formatted_date}' "
            f"WHERE file_name = '{filename}'"
        )

        update_statements.append(statement)

    logger.info(f"Updated statements created for staging table --- {update_statements}")

    logger.info("********** Connecting with MySQL server *****************")

    connection = get_mysql_connection()
    cursor = connection.cursor()

    logger.info("******* MySQL SERVER connected successfully **********")

    for statement in update_statements:
        cursor.execute(statement)

    connection.commit()

    cursor.close()
    connection.close()

else:
    logger.info("*********** There is some error in process in between ****************")
    sys.exit()



input("press Enter to terminate")


