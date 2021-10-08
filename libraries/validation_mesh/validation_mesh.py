import datetime
from google.cloud import storage
import pandas as pd
import numpy as np
import io
from google.cloud import bigquery

from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_PROYECTOS

def validation_mesh(esp_consolidado_corte):
    print(" *Validation Mesh Starting...");
    
    continue_process=True

    file_result_content="Resultado del proceso de ETL Prodesa\n\tFecha: " + datetime.date.today().strftime("%d-%m-%Y") + "\n\nCantidad de Registros en el archivo de carga: " + str(len(esp_consolidado_corte)) + "\n\nMalla de Validaciones:\n\n\t1- Verificación de Campos Vacíos:"

    # create in memory file
    output = io.StringIO(file_result_content)
    output.seek(0,2)

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
    file_result_content = "\n\n\t\t*Columna NAME - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Insertar el texto 'Actividad Nula' y reportar el hallazgo.\n\t\t\t\tDetiene el proceso: No \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)

    output.write(file_result_content)

    #Column ACTUAL_START_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.ACTUAL_START_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna ACTUAL_START_DATE - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)
    
    #Column ACTUAL_FINISH_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.ACTUAL_FINISH_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna ACTUAL_FINISH_DATE - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

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
    file_result_content = "\n\n\t\t*Columna DURATION_REMAINED - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Insertar el texto '1 dia' y reportar el hallazgo.\n\t\t\t\tDetiene el proceso: No \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Column LIKELY_START_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.LIKELY_START_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False
    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna LIKELY_START_DATE - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

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
    file_result_content = "\n\n\t\t*Columna DURATION - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Insertar el texto '1 dia' y reportar el hallazgo.\n\t\t\t\tDetiene el proceso: No \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Column LIKELY_FINISH_DATE - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.LIKELY_FINISH_DATE.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna LIKELY_FINISH_DATE - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Column FIN_LINEA_BASE_EST - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.FIN_LINEA_BASE_EST.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna FIN_LINEA_BASE_EST - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)
    
    #Column D_START - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.D_START.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna D_START - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Column D_FINISH - DATE
    #   * Stop the process and report the issue
    mistakes=esp_consolidado_corte[esp_consolidado_corte.D_FINISH.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna D_FINISH - DATE \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí \n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Validation: All the items are included in a list of valid items.
    file_result_content = "\n\n\n\t2- Validacion: Todos los items estan incluidos en una lista de items validos"
    output.write(file_result_content)

    #Column NOTE - STRING
    #   * Delete the values out of the reference set
    mistakes=esp_consolidado_corte[esp_consolidado_corte.D_FINISH.isnull()]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna NOTE - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Eliminar los valores fuera del conjunto de referencia.\n\t\t\t\tDetiene el proceso: No"
    output.write(file_result_content)
    
    #Validation: Not null values, and values are just "Sí" or "No"
    file_result_content = "\n\n\n\t3- Validacion: No debe haber valores Nulos. Los valores deben ser solamente 'Sí' o 'No'"
    output.write(file_result_content)

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
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna BUFFER - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí\n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

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
            text=text+", '"+(project_code)+"'"
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
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna PROJECT - STRING SubField Project code\n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí\n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)

    #Validating the structure of column
    file_result_content = "\n\n\n\t4- Validacion: Todos los registros deben tener la estructura <XXXXX>_<XXXXX>_<XXXXX>_<XXXXX>_<XXXXX>"
    output.write(file_result_content)

    #Column PROJECT - STRING
    #   * All the Items should have 4 '_' characters
    mistakes=esp_consolidado_corte[esp_consolidado_corte.PROJECT.str.count("_") != 4]
    findings = len(mistakes)
    mistakes = mistakes['PROJECT'].unique()
    happened=""
    if findings > 0:
        happened="Sí"
        continue_process=False

    else:
        happened="No"
    file_result_content = "\n\n\t\t*Columna PROJECT - STRING \n\t\t\tDescripción: \n\t\t\t\tAcciones: Reportar el hallazgo.\n\t\t\t\tDetiene el proceso: Sí\n\t\t\tResultado:\n\t\t\t\tDetectado: "+happened+"\n\t\t\t\tCantidad:"+str(findings)+"\n\t\t\t\t"+ str(mistakes)
    output.write(file_result_content)


    output.seek(0)


    #Storing the result to the bucket
    # bucket name
    bucket = "prodesa-biaas-salida"

    # Get the bucket that the file will be uploaded to.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)

    # Create a new blob and upload the file's content.
    my_file = bucket.blob('resultado_malla_validacion.txt')
    
    # upload from string
    my_file.upload_from_string(output.read(), content_type="text/plain")

    output.close()

    print(" -Validation Mesh ending...");
    return esp_consolidado_corte, continue_process