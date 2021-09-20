
from libraries.settings import ENVIRONMENT
from google.cloud import bigquery

def model(tmp_proyectos_construccion):

    print(" *Model Starting...")

    #TODO
    #   1. "Control de Hitos de Planeacion"
    #   2. "Consolidado de Proyectos de Planeacion"
    #   3. "Consolidado de Proyectos de Construccion"
    #   4. "Consolidado de Proyectos de Comercial"
    #   5. "Reporte por entrega"







    if ENVIRONMENT == "Production":
        #Persisting at BigQuery
        #modelo_biaas.tbl_inicio_venta

        client = bigquery.Client()
        table_id = 'modelo_biaas_python_test.tbl_proyectos_construccion_test2'
        # Since string columns use the "object" dtype, pass in a (partial) schema
        # to ensure the correct BigQuery data type.
        job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("tpc_regional",                    "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_codigo_proyecto",             "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_macroproyecto",               "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_proyecto",                    "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_etapa",                       "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_programacion",                "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_tarea_consume_buffer",        "STRING",   mode="NULLABLE"),
            bigquery.SchemaField("tpc_avance_cc",                   "FLOAT64",  mode="NULLABLE"),
            bigquery.SchemaField("tpc_avance_comparativo_semana",   "INT64",    mode="NULLABLE"),
            bigquery.SchemaField("tpc_consumo_buffer",              "FLOAT64",  mode="NULLABLE"),
            bigquery.SchemaField("tpc_consumo_buffer_comparativo",  "INT64",    mode="NULLABLE"),
            bigquery.SchemaField("tpc_fin_proyectado_optimista",    "DATE",     mode="NULLABLE"),
            bigquery.SchemaField("tpc_fin_proyectado_pesimista",    "DATE",     mode="NULLABLE"),
            bigquery.SchemaField("tpc_fin_programada",              "DATE",     mode="NULLABLE"),
            bigquery.SchemaField("tpc_dias_atraso",                 "INT64",    mode="NULLABLE"),
            bigquery.SchemaField("tpc_ultima_semana",               "FLOAT64",  mode="NULLABLE"),
            bigquery.SchemaField("tpc_ultimo_mes",                  "FLOAT64",  mode="NULLABLE"),
            bigquery.SchemaField("tpc_fecha_corte",                 "DATE",     mode="NULLABLE"),
            bigquery.SchemaField("tpc_fecha_proceso",               "DATETIME",     mode="NULLABLE"),
            bigquery.SchemaField("tpc_lote_proceso",                "INT64",    mode="NULLABLE"),
        ])


        job = client.load_table_from_dataframe(
            tmp_proyectos_construccion, table_id, job_config=job_config
        )

        # Wait for the load job to complete.
        job.result()
    print(" *Model ending...")