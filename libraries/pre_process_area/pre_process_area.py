from google.cloud import storage
import io
from datetime import datetime

from libraries.settings import AFFIRMATIVE, BLOB_NAME_VALIDATION_REPORT, BUCKET_NAME, BUCKET_NAME_VALIDATION_REPORT, NEGATIVE, NO_PASS, PASS
from libraries.various.store_process_result import store_process_result

def pre_process_area():
    print(" *Pre-Process Area Starting...");
    
    pre_process_result_content = "\t\t\t-- RESULTADO DE PROCESO DE ETL PRODESA --\n\nFecha de Proceso: "+ datetime.today().strftime('%d-%m-%Y') +"\n\n\t\t\t-- Resultado del proceso de Pre-Procesamiento --\n\nNombre del 'Bucket' de entrada configurado: "+ BUCKET_NAME
    output = io.StringIO(pre_process_result_content)
    output.seek(0,2)

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    detected= False
    stop_process= True
    for bucket in buckets:
        if bucket.name == BUCKET_NAME:
            detected=True
            stop_process= False

    pre_process_result_content = "\n\nVerificación de existencia de 'Bucket' de Entrada.\n\tResultado\n\t\tDetectado:" + (AFFIRMATIVE if detected == True else NEGATIVE) + "\n\t\tDetiene el proceso: " + (AFFIRMATIVE if stop_process == True else NEGATIVE)
    output.write(pre_process_result_content)

    if stop_process != True:
        bucket = storage_client.bucket(BUCKET_NAME)
        all_blobs = list(storage_client.list_blobs(bucket))
        
        detected= False
        stop_process= True
        
        if len(all_blobs) == 1:
            detected=True
            stop_process= False

        pre_process_result_content="\n\nVerificación de existencia de Archivo Fuente\n\tResultado\n\t\tCantidad de Archivos Detectados:" + str(len(all_blobs)) +"\n\t\tDetiene el proceso: " + (AFFIRMATIVE if stop_process == True else NEGATIVE) + "\n\t\tArchivos encontrados en el 'Bucket' de Entrada: "
        output.write(pre_process_result_content)

        for blob in all_blobs:
            pre_process_result_content= "\n\t\t\t"+str(blob.name)
            output.write(pre_process_result_content)
        
        if stop_process==True:
            for blob in all_blobs:
                blob.delete()

        #len(all_blobs)
    source_file_name=""
    if stop_process != True:
        source_file_name = all_blobs.pop().name
        splitted_source_file_name=source_file_name.split(".")
        detected= False
        stop_process= True
        if splitted_source_file_name[1]=="xlsx":
            detected= True
            stop_process= False
        pre_process_result_content="\n\nVerificación de extension del archivo Fuente\n\tResultado\n\t\tExtension del archivo: "+ splitted_source_file_name[1] +"\n\t\tDetiene el proceso: " + (AFFIRMATIVE if stop_process == True else NEGATIVE)
        output.write(pre_process_result_content)

    pre_process_result_content="\n\nResumen Pre-Procesamiento"
    output.write(pre_process_result_content)

    if stop_process != True:
        pre_process_result_content="\n\tNombre del Archivo Fuente: " + source_file_name
        output.write(pre_process_result_content)

    pre_process_result_content="\n\tResultado de Proceso de Pre-Procesamiento: "+ (NO_PASS if stop_process == True else PASS)
    output.write(pre_process_result_content)
    

    if stop_process == True:
        store_process_result(output)


    print(" -Pre-Process Area ending...");
    return stop_process, output, source_file_name