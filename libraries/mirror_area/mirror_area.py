
from libraries.settings import BLOB_NAME, BUCKET_NAME, ENVIRONMENT,ORIGIN_FILE 
import pandas as pd
from google.cloud import storage
import io


def mirror_area():
    print(" *Mirror Area Starting...");
    
    if ENVIRONMENT == "Development":
        #Uploading Consolidado
        #esp_consolidado_corte = pd.read_excel(ORIGIN_FILE);

        bucket_name= BUCKET_NAME
        blob_namne = BLOB_NAME
        #Taking the File from the Input Bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_namne)

        data_bytes = blob.download_as_bytes()

        esp_consolidado_corte = pd.read_excel(data_bytes)
        print(esp_consolidado_corte.head)
    else:
        bucket_name= BUCKET_NAME
        blob_namne = BLOB_NAME
        #Taking the File from the Input Bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_namne)

        data_bytes = blob.download_as_bytes()

        esp_consolidado_corte = pd.read_excel(data_bytes)

    print(" -Mirror Area ending...");

    return esp_consolidado_corte;
    