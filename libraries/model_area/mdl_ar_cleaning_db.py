from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_DESCARGA_REPORTES, TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER, TBL_INICIO_CONSTRUCCION, TBL_INICIO_ESCRITURACION, TBL_INICIO_PROMESA, TBL_INICIO_VENTA, TBL_PROYECTOS_COMERCIAL, TBL_PROYECTOS_CONSTRUCCION, TBL_PROYECTOS_PLANEACION, TBL_REPORTE_POR_ENTREGAS

from google.cloud import bigquery
import pandas as pd

def mdl_ar_cleaning_db(cut_date):
    print("  *Cleaning DataBase Process Starting")

    client = bigquery.Client()
    
    #Cleaning table tbl_graficos_tiempo_avance_buffer
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER + """`
            WHERE tgabt_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_proyectos_construccion
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_CONSTRUCCION + """`
            WHERE tpc_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_reporte_por_entregas
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_REPORTE_POR_ENTREGAS + """`
            WHERE trpe_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_proyectos_planeacion 
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_PLANEACION + """`
            WHERE tpp_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_proyectos_comercial
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_COMERCIAL + """`
            WHERE tpcm_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_inicio_venta
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_INICIO_VENTA + """`
            WHERE tiv_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_inicio_promesa
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_INICIO_PROMESA + """`
            WHERE tip_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_inicio_construccion
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_INICIO_CONSTRUCCION + """`
            WHERE tic_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    #Cleaning table tbl_inicio_escrituracion
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_INICIO_ESCRITURACION + """`
            WHERE tie_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)
    
    #Cleaning table tbl_descarga_reportes
    query ="""
        DELETE
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_DESCARGA_REPORTES + """`
            WHERE tdr_fecha_corte >= DATE '""" + cut_date +"""'
            """
    #print(query)        
    client.query(query)

    print("  -Cleaning DataBase Process ending") 

    return