'''
Author - Aditya Bhatt 7:00 AM
Objectives - 
1.Write code to read all files from raw container and then dump the same to processed container after applying the following transformations-
1.1 Remove Null's(This item to be handeled by data science team)
1.2 Extract Month from from_date
1.3 Drop from_date and to_date
1.4 Add a column for State (Not Sure will add this is this code to later)
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
CONTAINERNAME= "raw"

ACCOUNT_NAME="dataairquality"
ACCOUNT_KEY="R0zWp7D/7qFb1y4nkNg1gFkJepOmjnb57SjDqv7KjhOej/DwJgRS/uLn1/r1tK08BrEN5/Z103L8+AStJFr8Eg=="
DEST_CONTAINER="processed"




# Connect to Blob Storage
blob_service_client = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

# Get a reference to the container
container_client = blob_service_client.get_container_client(CONTAINERNAME)

# List all blobs in the container
blob_list = [blob.name for blob in container_client.list_blobs()]


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

    meta_list = []

    for i in df.columns:
        if i in  ['From Date','PM2.5 (ug/m3)','PM10 (ug/m3)','NO (ug/m3)','NO2 (ug/m3)','NOx (ppb)','NH3 (ug/m3)','SO2 (ug/m3)','CO (mg/m3)','Ozone (ug/m3)'	,"Benzene (ug/m3)"]:
            meta_list.append("+1")

    if len(meta_list)==11:
               
                df=df[['From Date','PM2.5 (ug/m3)','PM10 (ug/m3)','NO (ug/m3)','NO2 (ug/m3)','NOx (ppb)','NH3 (ug/m3)','SO2 (ug/m3)','CO (mg/m3)','Ozone (ug/m3)'	,"Benzene (ug/m3)"]]
              
                df['From Date'] = pd.to_datetime(df['From Date'])
                df['Month'] = df['From Date'].dt.month
                df['filename']=BLOBNAME.split(".")[0]
                df.drop(['From Date'],axis=1,inplace=True)
                #Keeping the nulls in the dataset , onus on the data science team
                #df=df.dropna()
                
                
                output = df.to_csv (encoding = "utf-8",index=False)


                #Pushing the data back to processed Container
                blob_service = BlobServiceClient.from_connection_string(
                    f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey= {ACCOUNT_KEY};EndpointSuffix=core.windows.net"
                )

                container_client = blob_service.get_container_client(DEST_CONTAINER)
                blob_client = blob_service.get_blob_client(container=DEST_CONTAINER, blob="t_"+f"{j}") 

                blob_client.upload_blob(output,overwrite=True,content_settings=ContentSettings(content_type="text/csv"))

