


from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_INICIO_CONSTRUCCION
from google.cloud import bigquery

import pandas as pd

def mdl_ar_mlstns_inicio_construccion(tbl_inicio_construccion):
    print("  *Model -tbl_inicio_construccion- Starting")

    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tic_regional",                        "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tic_codigo_proyecto",                 "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tic_macroproyecto",                   "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tic_proyecto",                        "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tic_etapa",                           "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tic_dias_atraso",                     "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tic_inicio_construccion_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_inicio_construccion_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_proc_contratacion_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_proc_contratacion_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_entrega_kit2_proyectado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_entrega_kit2_programado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_entrega_kit1_proyectado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_entrega_kit1_programado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_factib_inic_constru_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_factib_inic_constru_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_ppto_definitivo_proyectado",      "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_ppto_definitivo_programado",      "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_ppto_sipro_proyectado",           "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_ppto_sipro_programado",           "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_docs_inic_constru_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_docs_inic_constru_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_diseno_inic_constru_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_diseno_inic_constru_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tic_fecha_corte",                     "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tic_fecha_proceso",                   "DATE", mode="REQUIRED"),
        bigquery.SchemaField("tic_lote_proceso",                    "INT64",    mode="REQUIRED"),
    ])

    job = client.load_table_from_dataframe(
        tbl_inicio_construccion, TBL_INICIO_CONSTRUCCION, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_inicio_construccion- ending") 