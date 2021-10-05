import pandas as pd
import numpy as np

def validation_mesh(esp_consolidado_corte):
    print(" *Validation Mesh Starting...");
    continue_process=True

    #TODO:
    #Empty Fields verification in Cloumns
    #   *Column NAME - STRING
    #       - Insert the text "Actividad Nula" and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.NAME.isnull()])>0:
        continue_process=False
        print("Columna NAME tiene valores nulos. Resultado: 'Actividad Nula' Agregado a los campos. El proceso no fue detenido")
    esp_consolidado_corte['NAME']=np.where(esp_consolidado_corte['NAME']=="","Actividad Nula",esp_consolidado_corte['NAME'])
    #   *Column ACTUAL_START_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.ACTUAL_START_DATE.isnull()])>0:
        continue_process=False
        print("Columna ACTUAL_START_DATE tiene valores nulos")
    #   *Column ACTUAL_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.ACTUAL_FINISH_DATE.isnull()])>0:
        continue_process=False
        print("Columna ACTUAL_FINISH_DATE tiene valores nulos")
    #   *Column DURATION_REMAINED - STRING
    #       - Insert the text "1 dia" and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.DURATION_REMAINED.isnull()])>0:
        continue_process=False
        print("Columna DURATION_REMAINED tiene valores nulos. Resultado: '1 dia' Agregado a los campos. El proceso no fue detenido")
    esp_consolidado_corte['DURATION_REMAINED']=np.where(esp_consolidado_corte['DURATION_REMAINED']=="","1 dia",esp_consolidado_corte['DURATION_REMAINED'])
    #   *Column LIKELY_START_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.LIKELY_START_DATE.isnull()])>0:
        continue_process=False
        print("Columna LIKELY_START_DATE tiene valores nulos")
    #   *Column DURATION - STRING
    #       - Insert the text "1 dia" and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.DURATION.isnull()])>0:
        continue_process=False
        print("Columna DURATION tiene valores nulos. Resultado: '1 dia' Agregado a los campos. El proceso no fue detenido")
    esp_consolidado_corte['DURATION']=np.where(esp_consolidado_corte['DURATION']=="","1 dia",esp_consolidado_corte['DURATION'])
    #   *Column LIKELY_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.LIKELY_FINISH_DATE.isnull()])>0:
        continue_process=False
        print("Columna LIKELY_FINISH_DATE tiene valores nulos")
    #   *Column FIN_LINEA_BASE_EST - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.FIN_LINEA_BASE_EST.isnull()])>0:
        continue_process=False
        print("Columna FIN_LINEA_BASE_EST tiene valores nulos")
    #   *Column D_START - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.D_START.isnull()])>0:
        continue_process=False
        print("Columna D_START tiene valores nulos")
    #   *Column D_FINISH - DATE
    #       - Stop the process and report the issue
    if len(esp_consolidado_corte[esp_consolidado_corte.D_FINISH.isnull()])>0:
        continue_process=False
        print("Columna D_FINISH tiene valores nulos")
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
    print(" -Validation Mesh ending...");
    return esp_consolidado_corte, continue_process