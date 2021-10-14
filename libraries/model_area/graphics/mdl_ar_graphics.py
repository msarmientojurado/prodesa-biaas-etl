from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER, TBL_PROYECTOS_PLANEACION
from google.cloud import bigquery

import pandas as pd
def mdl_ar_graphics(tbl_graficos_tiempo_avance_buffer):

    print("  *Model -tbl_graficos_tiempo_avance_buffer- Starting")
    
    
    client = bigquery.Client()
    cut_date = pd.to_datetime((tbl_graficos_tiempo_avance_buffer.tgabt_fecha_corte.unique())[0])
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER + """`
            WHERE tgabt_fecha_corte >= DATE '""" + cut_date.strftime("%Y-%m-%d") +"""'
            """

    #print(query)        
    client.query(query)


    #client = bigquery.Client()
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("tgabt_area_prodesa",                      "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_regional",                          "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_codigo_proyecto",                   "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_macroproyecto",                     "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_proyecto",                          "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_etapa",                             "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_programacion",                      "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tgabt_avance_cc",                         "FLOAT64",  mode="REQUIRED"),
        bigquery.SchemaField("tgabt_consumo_buffer",                    "FLOAT64",  mode="REQUIRED"),
        bigquery.SchemaField("tgabt_fecha_inicio_linea_base",           "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tgabt_fecha_fin_linea_base",              "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tgabt_fecha_fin_buffer_linea_base",       "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tgabt_fecha_corte",                       "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tgabt_fecha_proceso",                     "DATE",     mode="REQUIRED"),
        bigquery.SchemaField("tgabt_lote_proceso",                      "INT64",    mode="REQUIRED"),
    ])
    
    job = client.load_table_from_dataframe(
        tbl_graficos_tiempo_avance_buffer, TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print("  -Model -tbl_graficos_tiempo_avance_buffer- ending")
    return