import os
from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()

local_file_path = "C:\\Users\\firoj\\OneDrive\\图片\\file\\"
def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
    except Exception as e:
        raise e

s3_directory = "sales_data/"
s3_bucket = "de-golu-project-1"
upload_to_s3(s3_directory, s3_bucket, local_file_path)


import os
from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *

s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key),
    decrypt(config.aws_secret_key)
)

s3_client = s3_client_provider.get_client()

local_folder_path = r"C:\Users\firoj\Music\spark_data\sales_data"

def upload_to_s3(s3_directory, s3_bucket, local_folder_path):

    try:
        for root, dirs, files in os.walk(local_folder_path):

            for file in files:

                print("Uploading:", file)

                full_local_path = os.path.join(root, file)

                s3_key = f"{s3_directory}{file}"

                s3_client.upload_file(
                    full_local_path,
                    s3_bucket,
                    s3_key
                )

                print(f"Uploaded -> {s3_key}")

    except Exception as e:
        print("Upload Error:", e)
        raise e

s3_directory = "sales_data/"
s3_bucket = "de-golu-project-1"

upload_to_s3(
    s3_directory,
    s3_bucket,
    local_folder_path
)


# import os
# from resources.dev import config
# from src.main.utility.s3_client_object import *
# from src.main.utility.encrypt_decrypt import *
#
# s3_client_provider = S3ClientProvider(
#     decrypt(config.aws_access_key),
#     decrypt(config.aws_secret_key)
# )
#
# s3_client = s3_client_provider.get_client()
#
# local_folder_path = r"C:\Users\firoj\Music\spark_data\sales_data"
#
# def upload_to_s3():
#
#     for root, dirs, files in os.walk(local_folder_path):
#
#         print("FILES:", files)
#
#         for file in files:
#
#             full_local_path = os.path.join(root, file)
#
#             s3_key = f"sales_data/{file}"
#
#             print("Uploading:", full_local_path)
#
#             s3_client.upload_file(
#                 full_local_path,
#                 "de-golu-project-1",
#                 s3_key
#             )
#
#             print("Uploaded Successfully:", s3_key)
#
# upload_to_s3()