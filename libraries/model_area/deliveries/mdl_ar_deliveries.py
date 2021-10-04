
from libraries.settings import TBL_REPORTE_POR_ENTREGAS
from google.cloud import bigquery

def mdl_ar_deliveries(tbl_reporte_por_entrega):
    print("  *Model -tbl_reporte_por_entrega- Starting")
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("trpe_regional",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("trpe_codigo_proyecto",             "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("trpe_macroproyecto",               "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("trpe_proyecto",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("trpe_programacion",                "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("trpe_tarea_entrega",               "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("trpe_etapa",                       "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("trpe_entrega_real",                "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("trpe_entrega_programada",          "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("trpe_fecha_corte",                 "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("trpe_fecha_proceso",               "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("trpe_lote_proceso",                "INT64",    mode="REQUIRED")
    ])

    job = client.load_table_from_dataframe(
        tbl_reporte_por_entrega, TBL_REPORTE_POR_ENTREGAS, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_reporte_por_entrega- ending")
    return
