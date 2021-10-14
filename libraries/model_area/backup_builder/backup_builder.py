from datetime import datetime
from google.cloud import storage
from datetime import datetime

from libraries.settings import BUCKET_NAME_BACKUP_FILE

def backup_builder(archive, cut_date, bash):

    storage_client = storage.Client()
    
    object_name = "corte_" +  cut_date.strftime('%d-%m-%Y') + "_proceso_" + datetime.today().strftime('%d-%m-%Y')+ "_lote_" + str(bash) + ".xlsx"
    bucket = storage_client.bucket(BUCKET_NAME_BACKUP_FILE)

    blob = storage.Blob(object_name, bucket)
    #blob.upload_from_file(archive, content_type='application/xlsx')
    blob.upload_from_string(archive, content_type='xlsx')
    return object_name