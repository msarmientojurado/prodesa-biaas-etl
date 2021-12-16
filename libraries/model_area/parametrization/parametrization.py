
from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_CONTROL_CARGUE
from google.cloud import bigquery

from datetime import date

def parametrization():
    client = bigquery.Client()

    query ="""
        SELECT max(control_carge.tcc_fecha_proceso) as ultima_fecha_cargue, max(control_carge.tcc_lote_proceso) as ultimo_lote_cargue
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_CONTROL_CARGUE + """` control_carge
            INNER JOIN (
                SELECT MAX(tcc_fecha_proceso) as ultima_fecha
                    FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_CONTROL_CARGUE + """`
                    )sub
            on control_carge.tcc_fecha_proceso= sub.ultima_fecha
        """
    last_bash= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))
    
    if last_bash.ultima_fecha_cargue[0] != date.today():
        current_bash=1
    else:
        current_bash = last_bash.ultimo_lote_cargue[0] + 1

    #print(current_bash)
    
    return current_bash