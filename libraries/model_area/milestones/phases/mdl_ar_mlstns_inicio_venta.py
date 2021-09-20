

from libraries.settings import TBL_INICIO_VENTA
from google.cloud import bigquery

def mdl_ar_mlstns_inicio_venta(tbl_inicio_venta):
    print("  *Model -tbl_inicio_venta- Starting")
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tiv_regional",                    "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tiv_codigo_proyecto",             "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tiv_macroproyecto",               "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tiv_proyecto",                    "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tiv_etapa",                       "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("tiv_dias_atraso",                 "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tiv_inicio_ventas_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_inicio_ventas_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_fiv_proyectado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_fiv_programado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_ppto_revisado_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_ppto_revisado_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_docs_ppto_proyectado",        "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_docs_ppto_programado",        "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_encargo_fiduc_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_encargo_fiduc_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_kit_comercial_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_kit_comercial_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_val_sv_model_proyectado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_val_sv_model_programado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_const_sv_model_proyectado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_const_sv_model_programado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_aprobac_lc_proyectado",       "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_aprobacion_lc_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_radicacion_lc_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_salida_ventas_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_salida_ventas_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_acta_constituc_proyectado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_acta_constituc_programado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_visto_bueno_proyectado",      "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_visto_bueno_programado",      "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_elab_alternativ_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_elab_alternativ_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_prod_objetivo_proyectado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_prod_objetivo_programado",    "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_lluvia_ideas_proyectado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_lluvia_ideas_programado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_fecha_corte",                 "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tiv_fecha_proceso",               "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("tiv_lote_proceso",                "INT64",    mode="NULLABLE"),
    ])

    job = client.load_table_from_dataframe(
        tbl_inicio_venta, TBL_INICIO_VENTA, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_inicio_venta- ending") 
