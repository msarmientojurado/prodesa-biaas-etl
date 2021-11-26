import pandas as pd
from google.cloud import bigquery

from libraries.settings import TBL_DESCARGA_REPORTES


def download_reports(report_url, cut_date, bash):
    print("  *Model -tbl_descarga_reportes- Starting")

    data = {'tdr_enlace_descarga':  [report_url],
        'tdr_fecha_corte': [cut_date],
        'tdr_fecha_proceso': [pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))],
        'tdr_lote_proceso': [bash]
        
        }

    tbl_descarga_reportes = pd.DataFrame(data)
    
    
    client = bigquery.Client()

    #client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tdr_enlace_descarga",  "STRING",      mode="REQUIRED"),
        bigquery.SchemaField("tdr_fecha_corte",      "DATE",        mode="REQUIRED"),
        bigquery.SchemaField("tdr_fecha_proceso",    "DATE",        mode="REQUIRED"),
        bigquery.SchemaField("tdr_lote_proceso",     "INTEGER",     mode="REQUIRED"),
        ])
    
    job = client.load_table_from_dataframe(
        tbl_descarga_reportes, TBL_DESCARGA_REPORTES, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_descarga_reportes- ending")
    return