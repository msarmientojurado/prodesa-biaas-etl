
from libraries.settings_ import ORIGIN_FILE
from libraries.settings import BLOB_NAME, BUCKET_NAME, ENVIRONMENT
import pandas as pd
from google.cloud import storage

def mirror_area(file_name):
    print(" *Mirror Area Starting...");
    
    if ENVIRONMENT == "Development":
        #Uploading Consolidado
        esp_consolidado_corte = pd.read_excel(ORIGIN_FILE);
    else: 
        #Taking the File from the Input Bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)

        data_bytes = blob.download_as_bytes()

        esp_consolidado_corte = pd.read_excel(data_bytes)

    print(" -Mirror Area ending...");

    return esp_consolidado_corte;
    