
from libraries.settings import TBL_PROYECTOS_COMERCIAL
from google.cloud import bigquery

def mdl_ar_commercial(tmp_proyectos_comercial):
    print("  *Model -tbl_proyectos_comercial- Starting")
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tpcm_regional",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_codigo_proyecto",             "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_macroproyecto",               "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_proyecto",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_etapa",                       "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_programacion",                "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tpcm_tarea_consume_buffer",        "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpcm_avance_cc",                   "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpcm_avance_comparativo_semana",   "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpcm_consumo_buffer",              "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpcm_consumo_buffer_color",        "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpcm_consumo_buffer_comparativo",  "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpcm_fin_proyectado_optimista",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpcm_fin_proyectado_pesimista",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpcm_fin_programada",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpcm_dias_atraso",                 "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpcm_ultima_semana",               "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpcm_ultimo_mes",                  "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpcm_fecha_corte",                 "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tpcm_fecha_proceso",               "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tpcm_lote_proceso",                "INT64",    mode="REQUIRED"),
    ])

    job = client.load_table_from_dataframe(
        tmp_proyectos_comercial, TBL_PROYECTOS_COMERCIAL, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_proyectos_comercial- ending")