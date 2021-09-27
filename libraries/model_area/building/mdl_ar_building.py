from libraries.settings import TBL_PROYECTOS_CONSTRUCCION
from google.cloud import bigquery

def mdl_ar_building(tmp_proyectos_construccion):
    print("  *Model -tbl_proyectos_construccion- Starting")
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tpc_regional",                    "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_codigo_proyecto",             "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_macroproyecto",               "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_proyecto",                    "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_etapa",                       "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_programacion",                "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_tarea_consume_buffer",        "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tpc_avance_cc",                   "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpc_avance_comparativo_semana",   "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpc_consumo_buffer",              "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpc_consumo_buffer_color",        "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpc_consumo_buffer_comparativo",  "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpc_fin_proyectado_optimista",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpc_fin_proyectado_pesimista",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpc_fin_programada",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpc_dias_atraso",                 "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tpc_ultima_semana",               "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpc_ultimo_mes",                  "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("tpc_fecha_corte",                 "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tpc_fecha_proceso",               "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("tpc_lote_proceso",                "INT64",    mode="NULLABLE"),
    ])

    job = client.load_table_from_dataframe(
        tmp_proyectos_construccion, TBL_PROYECTOS_CONSTRUCCION, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_proyectos_construccion- ending")