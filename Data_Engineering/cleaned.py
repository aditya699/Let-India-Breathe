'''
Author - Aditya Bhatt 7:32
Objective -
1.Read all files from the processed container and merge_them into a single file and pushed to cleaned container.
'''
#Import necessary Modules
from azure.storage.blob import BlobServiceClient
import pandas as pd
import time
from io import BytesIO
from azure.storage.blob import *

#GLOBAL VARIABLES
STORAGEACCOUNTURL= "https://dataairquality.blob.core.windows.net/"
STORAGEACCOUNTKEY= "R0zWp7D/7qFb1y4nkNg1gFkJepOmjnb57SjDqv7KjhOej/DwJgRS/uLn1/r1tK08BrEN5/Z103L8+AStJFr8Eg=="
CONTAINERNAME= "processed"

ACCOUNT_NAME="dataairquality"
ACCOUNT_KEY="R0zWp7D/7qFb1y4nkNg1gFkJepOmjnb57SjDqv7KjhOej/DwJgRS/uLn1/r1tK08BrEN5/Z103L8+AStJFr8Eg=="
DEST_CONTAINER="cleaned"
# Connect to Blob Storage
blob_service_client = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

# Get a reference to the container
container_client = blob_service_client.get_container_client(CONTAINERNAME)

# List all blobs in the container
blob_list = [blob.name for blob in container_client.list_blobs()]

combined_df=pd.DataFrame()

#Iterating through the list of blobs and applying the transformations and dumping the same in Processed Container
for j in blob_list:
    BLOBNAME=j
    #download from blob
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    blob_data = blob_client_instance.download_blob()
    content = blob_data.readall()

    #Convert data to a format that is understood by pandas
    content_io = BytesIO(content)

    # Perform transformations using pandas
    df = pd.read_csv(content_io) 
    combined_df = combined_df.append(df, ignore_index=True)

cleaned_content = combined_df.to_csv(index=False, encoding='utf-8')

#Pushing the data back to processed Container
blob_service = BlobServiceClient.from_connection_string(
                    f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey= {ACCOUNT_KEY};EndpointSuffix=core.windows.net"
                )

container_client = blob_service.get_container_client(DEST_CONTAINER)
blob_client = blob_service.get_blob_client(container=DEST_CONTAINER, blob="concat_ai") 

blob_client.upload_blob(cleaned_content,overwrite=True,content_settings=ContentSettings(content_type="text/csv"))
 
