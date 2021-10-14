import pandas as pd
from google.cloud import bigquery

from libraries.settings import TBL_CONTROL_CARGUE

def loading_control(source_filename, backup_filename, bash, rows_loaded):
    print("  *Model -tbl_control_cargue- Starting")

    data = {'tcc_nombre_fuente':  [source_filename],
        'tcc_nombre_backup': [backup_filename],
        'tcc_fecha_proceso': [pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))],
        'tcc_lote_proceso': [bash],
        'tcc_cantidad_registros': [rows_loaded]
        
        }

    tbl_control_cargue = pd.DataFrame(data)
    
    
    client = bigquery.Client()

    #client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tcc_nombre_fuente",        "STRING",      mode="REQUIRED"),
        bigquery.SchemaField("tcc_nombre_backup",        "STRING",      mode="REQUIRED"),
        bigquery.SchemaField("tcc_fecha_proceso",        "DATE",        mode="REQUIRED"),
        bigquery.SchemaField("tcc_lote_proceso",         "INTEGER",     mode="REQUIRED"),
        bigquery.SchemaField("tcc_cantidad_registros",   "INTEGER",     mode="REQUIRED")
        ])
    
    job = client.load_table_from_dataframe(
        tbl_control_cargue, TBL_CONTROL_CARGUE, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_control_cargue- ending")
    return