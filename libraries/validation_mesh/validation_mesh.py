import datetime
from google.cloud import storage
import pandas as pd
import numpy as np
import io
from google.cloud import bigquery

from libraries.settings import AFFIRMATIVE, BIGQUERY_ENVIRONMENT_NAME, BLOB_NAME_VALIDATION_REPORT, BUCKET_NAME_VALIDATION_REPORT, NEGATIVE, NO_PASS, PASS, TBL_PROYECTOS, TBL_VALORES_HITOS
from libraries.various.store_process_result import store_process_result

def validation_mesh(esp_consolidado_corte, output_file_content):
    print(" *Validation Mesh Starting...");

    #file_result_content="\n\nCantidad de Registros en el archivo de carga: " + str(len(esp_consolidado_corte)) + "\n\nMalla de Validaciones:\n\n\t1- Verificación de Campos Vacíos:"
    file_result_content = "\n\n\t\t-- Resultado de Malla de Validaciones --"
    # create in memory file
    output_file_content.write(file_result_content)

    expected_columns = ['ID', 'WBS', 'NAME', 'ACTUAL_START_DATE', 'ACTUAL_FINISH_DATE',
       'DURATION_REMAINED', 'LIKELY_START_DATE', 'DURATION',
       'LOW_RISK_DURATION', 'RESOURCES', 'SUCCESORS', 'PROJECT_BUFFER_IMPACT',
       'INDICATOR', 'PCT_COMPLETED', 'BUFFER', 'BUFFER_INDEX',
       'CRITICAL_NUMBER', 'TASK', 'CRITICAL_TASK', 'STATE', 'SCHEME_NUMBER',
       'LIKELY_FINISH_DATE', 'PROJECT', 'D_START', 'D_FINISH', 'PREDECESSOR',
       'NOTE', 'DURACION_RESTANTE', 'FIN_LINEA_BASE_EST']

    real_columns = list(esp_consolidado_corte)
    
    if np.array_equal(expected_columns,real_columns):
        stop_process=False
    else:
        stop_process=True

    file_result_content="\n\nVerificacion de estructura del archivo cargado:\n\tResultado\n\t\tDetiene el proceso:" + (AFFIRMATIVE if stop_process == True else NEGATIVE)
    output_file_content.write(file_result_content)

    file_result_content="\n\n-- Conformidad de los Datos"
    output_file_content.write(file_result_content)
    
    file_result_content="\n\nCantidad de Registros en el archivo de carga: " + str(len(esp_consolidado_corte)) + "\n\n- Verificación de Campos Vacíos:"
    output_file_content.write(file_result_content)
    #Empty Fields verification in Cloumns

    #Column NAME - STRING
    #    * Insert the text "Actividad Nula" and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.NAME.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna NAME - STRING \n\t\tDescripción: \n\t\t\tAcciones: Insertar el texto 'Actividad Nula' y reportar el hallazgo.\n\t\t\tDetiene el proceso: No \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)

    output_file_content.write(file_result_content)

    esp_consolidado_corte['NAME']=np.where(esp_consolidado_corte.NAME.isnull(),"Actividad Nula",esp_consolidado_corte['NAME'])

    #Column ACTUAL_START_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.ACTUAL_START_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna ACTUAL_START_DATE - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column ACTUAL_FINISH_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.ACTUAL_FINISH_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True

    else:
        happened="No"
    file_result_content = "\n\n\t*Columna ACTUAL_FINISH_DATE - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column DURATION_REMAINED - STRING
    #   * Insert the text "1 dia" and report the issue    
    mistakes=esp_consolidado_corte[esp_consolidado_corte.DURATION_REMAINED.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna DURATION_REMAINED - STRING \n\t\tDescripción: \n\t\t\tAcciones: Insertar el texto '1 dia' y reportar el hallazgo.\n\t\t\tDetiene el proceso: No \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    esp_consolidado_corte['DURATION_REMAINED']=np.where(esp_consolidado_corte.DURATION_REMAINED.isnull(),"1 dia",esp_consolidado_corte['DURATION_REMAINED'])

    #Column LIKELY_START_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.LIKELY_START_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna LIKELY_START_DATE - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column DURATION - STRING
    #   * Insert the text "1 dia" and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.DURATION.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna DURATION - STRING \n\t\tDescripción: \n\t\t\tAcciones: Insertar el texto '1 dia' y reportar el hallazgo.\n\t\t\tDetiene el proceso: No \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    esp_consolidado_corte['DURATION']=np.where(esp_consolidado_corte.DURATION.isnull(),"1 dia",esp_consolidado_corte['DURATION'])

    #Column LIKELY_FINISH_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.LIKELY_FINISH_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna LIKELY_FINISH_DATE - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column FIN_LINEA_BASE_EST - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.FIN_LINEA_BASE_EST.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True

    else:
        happened="No"
    file_result_content = "\n\n\t*Columna FIN_LINEA_BASE_EST - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column D_START - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.D_START.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna D_START - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Column D_FINISH - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.D_FINISH.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True

    else:
        happened="No"
    file_result_content = "\n\n\t*Columna D_FINISH - DATE \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí \n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Validation: All the items are included in a list of valid items.
    file_result_content = "\n\n\n2- Validacion: Todos los items estan incluidos en una lista de items validos"
    output_file_content.write(file_result_content)

    #Column NOTE - STRING
    #   * Delete the values out of the reference set
    client = bigquery.Client()

    query ="""
        SELECT tvh_sigla,
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_VALORES_HITOS + """`
            WHERE tvh_estado = TRUE
            order by tvh_sigla desc
        """

    #print(query)        
    milestones_set= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))['tvh_sigla'].unique()
    esp_consolidado_corte['NOTE'] = esp_consolidado_corte['NOTE'].astype(str)
    
    #milestones_set=['GASUE','IV','IP','IC','IE','DC','GAS','AC','EL','SP']
    auxCol=pd.DataFrame()
    file_result_content = "\n\n\t*Columna NOTE - STRING \n\t\tDescripción: \n\t\t\tAcciones: Eliminar los valores fuera del conjunto de referencia.\n\t\t\tDetiene el proceso: No\n\t\tResultado:\n\t\t\tCantidades de filas detectadas por hito:"
    output_file_content.write(file_result_content)
    
    first_time_loop=True
    for milestone in milestones_set:
        res=esp_consolidado_corte['NOTE'].str.contains(milestone)
        file_result_content = "\n\t\t\t\t"+ milestone +"= "+ str(res.sum())
        output_file_content.write(file_result_content)
        banned = [milestone]
        f = lambda x: ' '.join([item for item in x.split() if item not in banned])
        esp_consolidado_corte['NOTE'] = esp_consolidado_corte['NOTE'].apply(f)
        if first_time_loop:
            auxCol['stg_notas']=np.where(res==True,milestone,"")
            first_time_loop=False
        else:
            auxCol['stg_notas']=np.where(res==True,(auxCol['stg_notas']+"-"+milestone),auxCol['stg_notas'])
    esp_consolidado_corte['NOTE'] = auxCol['stg_notas'].apply(lambda x : x[1:] if x.startswith("-") else x)


    #Validation: Not null values, and values are just "Sí" or "No"
    file_result_content = "\n\n\n3- Validacion: No debe haber valores Nulos. Los valores deben ser solamente 'Sí' o 'No'"
    output_file_content.write(file_result_content)

    #Column BUFFER - STRING
    #   * Stop the process and report the issue
    #TODO Build findings 
    list_of_values=["Sí", "No"]
    mistakes= esp_consolidado_corte[esp_consolidado_corte.BUFFER.isnull() | ~(esp_consolidado_corte['BUFFER'].isin(list_of_values))]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna BUFFER - STRING \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí\n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Colum PROJECT - STRING SubField Project code
    #   * Stop the process and report the issue
    client = bigquery.Client()
    auxCol=esp_consolidado_corte["PROJECT"].str.split("_", n=5,expand=True)
    project_codes=auxCol[1].unique()
    text=""
    for project_code in project_codes:
        if text== "":
            text=text+"'"+str(project_code)+"'"
        else:
            text=text+", '"+str(project_code)+"'"
    query ="""
        SELECT tpr_codigo_proyecto, (1) AS tpr_in_table
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS + """`
            WHERE tpr_codigo_proyecto in ("""+ text +""")
            and tpr_estado = TRUE
        """

    #print(query)        
    tbl_proyectos= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))

    project_codes=tbl_proyectos.tpr_codigo_proyecto.unique()
    mistakes=auxCol[~auxCol[1].isin(project_codes)]
    findings=len(mistakes)
    mistakes = mistakes[1].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"
    file_result_content = "\n\n\t*Columna PROJECT - STRING /SubCampo Codigo-Proyecto\n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí\n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    #Validating the structure of column
    file_result_content = "\n\n\n4- Validacion: Todos los registros deben tener la estructura <XXXXX>_<XXXXX>_<XXXXX>_<XXXXX>_<XXXXX>"
    output_file_content.write(file_result_content)

    #Column PROJECT - STRING
    #   * All the Items should have 4 '_' characters
    mistakes=esp_consolidado_corte[esp_consolidado_corte.PROJECT.str.count("_") != 4]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        stop_process=True
    else:
        happened="No"

    file_result_content = "\n\n\t*Columna PROJECT - STRING \n\t\tDescripción: \n\t\t\tAcciones: Reportar el hallazgo.\n\t\t\tDetiene el proceso: Sí\n\t\tResultado:\n\t\t\tDetectado: "+happened+"\n\t\t\tCantidad:"+str(findings)+"\n\t\t\t"+ str(mistakes)
    output_file_content.write(file_result_content)

    file_result_content = "\n\nResultado final de la malla de Validaciones: "+ (NO_PASS if stop_process == True else PASS)
    output_file_content.write(file_result_content)

    if stop_process == True:
        store_process_result(output_file_content)
    

    print(" -Validation Mesh ending...");
    return esp_consolidado_corte, stop_process, output_file_content