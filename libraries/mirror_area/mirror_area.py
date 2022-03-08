
from libraries.settings import AFFIRMATIVE, BLOB_NAME, BUCKET_NAME, ENVIRONMENT, NEGATIVE,ORIGIN_FILE 
import pandas as pd
import io
from google.cloud import storage

from libraries.various.store_process_result import store_process_result


def mirror_area(blob_namne, report_file_content):
    '''This function implements the extraction process: Loads the excel file into a DataFrame'''
    print(" *Mirror Area Starting...");

    bucket_name= BUCKET_NAME
    #blob_namne = BLOB_NAME
    #Taking the File from the Input Bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_namne)

    data_bytes = blob.download_as_bytes()

    blob.delete()

    stop_process=True

    esp_consolidado_corte = pd.DataFrame()
    try:
        esp_consolidado_corte = pd.read_excel(data_bytes)
        stop_process=False
    except:
        stop_process = True
    
    pre_process_result_content = "\n\n\t\t\t-- Resultado de Cargue del Archivo Fuente --\n\nVerificacion de que pudo cargarse el archivo al proceso ETL\n\tResultado\n\t\tDetiene el proceso: "+ (AFFIRMATIVE if stop_process == True else NEGATIVE) + "\n\t\tNombre del Archivo Cargado: " + blob_namne
    report_file_content.write(pre_process_result_content)

    if stop_process == True:
        store_process_result(report_file_content)

    print(" -Mirror Area ending...");

    return esp_consolidado_corte, report_file_content, stop_process, data_bytes;
    