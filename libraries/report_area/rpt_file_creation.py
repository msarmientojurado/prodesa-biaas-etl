
from tempfile import NamedTemporaryFile

from libraries.settings import BUCKET_NAME_DOWNLOAD_REPORT, DOWNLOAD_REPORTS_FOLDER_URL
from google.cloud import storage
from zipfile import ZipFile

def rpt_file_creation(excel_report_array):
    print("  *File Creation Starting")
    cut_date=""
    with NamedTemporaryFile() as tmp_2:
        #wb.save(tmp_2.name)
        zipObj = ZipFile(tmp_2.name, 'w')
        #zipObj.write('prueba/out_template.txt')
        for report in excel_report_array:
            zipObj.writestr(report[1]+'/'+report[2],report[0].read())
            cut_date=report[3]
        zipObj.close()
        storage_client = storage.Client()

        object_name = "reportes_"+cut_date.strftime('%d-%m-%Y')+".zip"
        bucket = storage_client.bucket(BUCKET_NAME_DOWNLOAD_REPORT)

        blob = storage.Blob(object_name, bucket)
        #blob.upload_from_file(archive, content_type='application/xlsx')
        blob.upload_from_string(tmp_2.read(), content_type='application/zip')
    print("  -File Creation Ending")
    return DOWNLOAD_REPORTS_FOLDER_URL + object_name