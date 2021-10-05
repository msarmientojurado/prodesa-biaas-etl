from google.cloud import bigquery

from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_PROYECTOS

import numpy as np

def information_consistency(stg_consolidado_corte):

    continue_process = True

    #Validation: All the items are included in a list of valid items
    #   *PROJECT - STRING
    #       - Stop the process and report the issue
    client = bigquery.Client()
    project_codes=stg_consolidado_corte.stg_codigo_proyecto.unique()
    text=""
    for project_code in project_codes:
        if text== "":
            text=text+"'"+project_code+"'"
        else:
            text=text+", '"+project_code+"'"
    query ="""
        SELECT tpr_codigo_proyecto, (1) AS tpr_in_table
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS + """`
            WHERE tpr_codigo_proyecto in ("""+ text +""")
            and tpr_estado = TRUE
        """

    #print(query)        
    tbl_proyectos= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))
    #print(tbl_proyectos.head(5))
    project_codes=tbl_proyectos.tpr_codigo_proyecto.unique()
    tbl_proyectos=stg_consolidado_corte[~stg_consolidado_corte['stg_codigo_proyecto'].isin(project_codes)]
    print("   Codigos de Proyecto faltantes en la Tabla 'tbl_proyectos':")
    project_codes=tbl_proyectos.stg_codigo_proyecto.unique()
    #project_codes=[]
    print(project_codes)
    if len(project_codes)>0:
        continue_process=False
    return stg_consolidado_corte, continue_process