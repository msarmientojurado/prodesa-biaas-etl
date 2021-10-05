import pandas as pd
import numpy as np

def validation_mesh(esp_consolidado_corte):
    
    continue_process=True

    #TODO:
    #Empty Fields verification in Cloumns
    #   *Column NAME - STRING
    #       - Insert the text "Actividad Nula" and report the issue
    if len(esp_consolidado_corte.NAME.isnull())>0:
        continue_process=False
        print("Columna NAME tiene valores nulos. Resultado: 'Actividad Nula' Agregado a los campos. El proceso no fue detenido")
    esp_consolidado_corte['NAME']=np.where(esp_consolidado_corte['NAME']=="","Actividad Nula",esp_consolidado_corte['NAME'])
    #   *Column ACTUAL_START_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte.ACTUAL_START_DATE.isnull())>0:
        continue_process=False
        print("Columna ACTUAL_START_DATE tiene valores nulos")
    #   *Column ACTUAL_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte.ACTUAL_FINISH_DATE.isnull())>0:
        continue_process=False
        print("Columna ACTUAL_FINISH_DATE tiene valores nulos")
    #   *Column DURATION_REMAINED - STRING
    #       - Insert the text "1 dia" and report the issue
    #   *Column LIKELY_START_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column DURATION - STRING
    #       - Insert the text "1 dia" and report the issue
    #   *Column LIKELY_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column FIN_LINEA_BASE_EST - DATE
    #       - Stop the process and report the issue
    #   *Column D_START - DATE
    #       - Stop the process and report the issue
    #   *Column D_FINISH - DATE
    #       - Stop the process and report the issue
    #
    #           --------------------
    #
    #Validation: All the items are included in a list of valid items.
    #   *Column NOTE - STRING
    #       - Delete the values out of the reference set
    #
    #           --------------------
    #
    #Validation: Not null values, and values are just "Sí" or "No"
    #   *Column BUFFER - STRING
    #       - Stop the process and report the issue
    #   *Column TASK - STRING
    #       - Stop the process and report the issue
    #
    #           --------------------
    #
    return esp_consolidado_corte, continue_process