
from libraries.settings import ENVIRONMENT
import pandas as pd
from google.cloud import storage

def mirror_area(file_name):
    print(" *Mirror Area Starting...");
    
    if ENVIRONMENT == "Development":
        #Uploading Consolidado
        esp_consolidado_corte = pd.read_excel(file_name);
    else: 
        #Taking the File from the Input Bucket

        bucket_name = "prodesa-biaas-bucket"
        blob_name = "Consolidado_Excel_13-08-2021.xlsx"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        data_bytes = blob.download_as_bytes()

        esp_consolidado_corte = pd.read_excel(data_bytes)

    print(" -Mirror Area ending...");

    return esp_consolidado_corte;
    