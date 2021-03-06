from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_INICIO_ESCRITURACION
from google.cloud import bigquery
import pandas as pd

def mdl_ar_mlstns_inicio_escrituracion(tbl_inicio_escrituracion):
    print("  *Model -tbl_inicio_escrituracion- Starting")
    
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tie_regional",                            "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tie_codigo_proyecto",                     "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tie_macroproyecto",                       "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tie_proyecto",                            "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tie_etapa",                               "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tie_dias_atraso",                         "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tie_inicio_escrituracion_proyectado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_inicio_escrituracion_programado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_poder_fiduciaria_proyectado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_poder_fiduciaria_programado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_salida_rph_proyectado",               "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_salida_rph_programado",               "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_cierre_rph_proyectado",               "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_cierre_rph_programado",               "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_licencia_ph_proyectado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_licencia_ph_programado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_modificacion_lc_proyectado",          "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_modificacion_lc_programado",          "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_radic_modif_lc_proyectado",           "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_radic_modif_lc_programado",           "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tie_fecha_corte",                         "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tie_fecha_proceso",                       "DATE", mode="REQUIRED"),
        bigquery.SchemaField("tie_lote_proceso",                        "INT64",    mode="REQUIRED"),
    ])
    
    job = client.load_table_from_dataframe(
        tbl_inicio_escrituracion, TBL_INICIO_ESCRITURACION, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_inicio_escrituracion- ending") 