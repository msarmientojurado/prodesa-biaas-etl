

from libraries.settings import TBL_INICIO_PROMESA
from google.cloud import bigquery

def mdl_ar_mlstns_inicio_promesa(tbl_inicio_promesa):
    print("  *Model -tbl_inicio_promesa- Starting")
    client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tip_regional",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tip_codigo_proyecto",             "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tip_macroproyecto",               "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tip_proyecto",                    "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tip_etapa",                       "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tip_dias_atraso",                 "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("tip_inicio_promesas_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_inicio_promesas_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_ent_kit_prom_proyectado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_ent_kit_prom_programado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_permiso_ventas_proyectado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_permiso_ventas_programado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_min_hipo_reg_proyectado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_min_hipo_reg_programado",     "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_rad_min_hipo_reg_proyectado", "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_rad_min_hipo_reg_programado", "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_cred_construct_proyectado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_cred_construct_programado",   "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_linderos_proyectado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_linderos_programado",         "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_fai_proyectado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_fai_programado",              "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_constitut_urban_proyectado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_constitut_urban_programado",  "DATE",     mode="NULLABLE"),
        bigquery.SchemaField("tip_fecha_corte",                 "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tip_fecha_proceso",               "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("tip_lote_proceso",                "INT64",    mode="REQUIRED"),
    ])

    job = client.load_table_from_dataframe(
        tbl_inicio_promesa, TBL_INICIO_PROMESA, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_inicio_promesa- ending") 