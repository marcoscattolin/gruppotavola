import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from grptavutils.constants import Storage


def list_blob_files(container_name, blob_path):

    # init client
    with open("../../secrets/azure_creds.json", "r") as f:
        connection_string = json.load(f)
    connection_string = connection_string["connection_string"]
    blob_source_service_client = BlobServiceClient.from_connection_string(connection_string)
    source_container_client = blob_source_service_client.get_container_client(container_name)

    # get files
    blob_name_list = []
    source_blob_list = source_container_client.list_blobs(name_starts_with=blob_path)
    for f in source_blob_list:
        blob_name_list.append(f["name"])

    return blob_name_list


def delete_blob_file(container_name, file_path):

    # init client
    with open("../../secrets/azure_creds.json", "r") as f:
        connection_string = json.load(f)
    connection_string = connection_string["connection_string"]
    blob_source_service_client = BlobServiceClient.from_connection_string(connection_string)
    source_container_client = blob_source_service_client.get_container_client(container_name)

    # delete file
    source_container_client.delete_blob(file_path, snapshot=None)



def read_parquet(container, file_path):

    # get azure access credentials
    with open("../../secrets/azure_creds.json", "r") as f:
        storage_options = json.load(f)

    account_name = Storage.account_name
    df = pd.read_parquet(
        f"abfs://{container}@{account_name}.dfs.core.windows.net/{file_path}",
        storage_options=storage_options
    )

    return df


def read_csv(container, file_path):

    # get azure access credentials
    with open("../../secrets/azure_creds.json", "r") as f:
        storage_options = json.load(f)

    account_name = Storage.account_name
    df = pd.read_csv(
        f"abfs://{container}@{account_name}.dfs.core.windows.net/{file_path}",
        storage_options=storage_options
    )

    return df


def write_parquet(dataframe, container, file_path):

    # get azure access credentials
    with open("../../secrets/azure_creds.json", "r") as f:
        storage_options = json.load(f)

    account_name = Storage.account_name

    dataframe.to_parquet(
        f"abfs://{container}@{account_name}.dfs.core.windows.net/{file_path}",
        index=False,
        storage_options=storage_options
    )
