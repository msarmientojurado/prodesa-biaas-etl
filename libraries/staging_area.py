
import pandas as pd


def staging(esp_consolidado_corte):

    print(" *Staging Area Starting...");
    
    # *stg_consolidado_corte* will be the result of the staging area.
    stg_consolidado_corte=pd.DataFrame([],columns=['stg_project_id']);

    #Next you will find the columns and data type description of the *stg_consolidado_corte* DataFrame
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_project_id|integer|
    #    |stg_wbs|string|
    #    |stg_nombre_actividad|string|
    #    |stg_fecha_inicial_actual|date|
    #    |stg_fecha_final_actual|date|
    #    |stg_duracion_real_cantidad|float|
    #    |stg_duracion_real_unidad|string|
    #    |stg_fecha_inicio_planeada|date|
    #    |stg_duracion_cantidad|integer|
    #    |stg_duracion_unidad|string|
    #    |stg_riesgo_bajo_duracion_cantidad|integer|
    #    |stg_riesgo_bajo_duracion_unidad|string|
    #    |stg_recursos|string|
    #    |stg_actividad_sucesora|string|
    #    |stg_impacto_buffer|integer|
    #    |stg_indicador_cantidad|integer|
    #    |stg_indicador_unidad|string|
    #    |stg_porc_completitud|float|
    #    |stg_ind_buffer|string|
    #    |stg_indice_buffer|string|
    #    |stg_duracion_critica_cantidad|float|
    #    |stg_duracion_critica_unidad|string|
    #   |stg_ind_tarea|string|
    #    |stg_ind_tarea_critica|string|
    #    |stg_estado|integer|
    #    |stg_numero_esquema|string|
    #    |stg_fecha_fin_planeada|date|
    #    |stg_area_prodesa|string|
    #    |stg_nombre_archivo|string|
    #    |stg_codigo_proyecto|string|
    #    |stg_etapa_proyecto|string|
    #    |stg_programacion_proyecto|string|
    #    |stg_fecha_corte|date|
    #    |stg_fecha_inicial|datetime|
    #    |stg_fecha_fin|datetime|
    #    |stg_actividad_predecesora|string|
    #    |stg_notas|string|
    #    |stg_duracion_restante_cantidad|float|
    #    |stg_duracion_restante_unidad|string|
    #    |stg_fin_linea_base_estimado|date|
    #    |stg_fecha_fin_programada|date|
    #    |stg_fecha_proceso|date|
    #    |stg_lote_proceso|integer|

    #Lets start by the columns
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_project_id|integer|
    #    |stg_wbs|string|
    #    |stg_nombre_actividad|string|

    stg_consolidado_corte['stg_project_id']=pd.to_numeric(esp_consolidado_corte['ID'])
    stg_consolidado_corte['stg_wbs']=esp_consolidado_corte['WBS']
    stg_consolidado_corte['stg_nombre_actividad']=esp_consolidado_corte['NAME']
    
    #Now continue with the columns
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fecha_inicial_actual|date|
    #    |stg_fecha_final_actual|date|
    
    auxCol=esp_consolidado_corte['ACTUAL_START_DATE'].str.split(" ", n=2,expand=True)
    stg_consolidado_corte['stg_fecha_inicial_actual']=pd.to_datetime(auxCol[1], dayfirst=True)
    auxCol=esp_consolidado_corte['ACTUAL_FINISH_DATE'].str.split(" ", n=2,expand=True)
    stg_consolidado_corte['stg_fecha_final_actual']=pd.to_datetime(auxCol[1], dayfirst=True)
    
    #Now continue with the columns
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_duracion_real_cantidad|float|
    #    |stg_duracion_real_unidad|string|

    auxCol=esp_consolidado_corte['Duración_real'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_real_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_real_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fecha_inicio_planeada|date|

    auxCol=esp_consolidado_corte['LIKELY_START_DATE'].str.split(" ", n=2,expand=True)
    stg_consolidado_corte['stg_fecha_inicio_planeada']=pd.to_datetime(auxCol[1], dayfirst=True)
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_duracion_cantidad|integer|
    #    |stg_duracion_unidad|string|
    
    auxCol=esp_consolidado_corte['DURATION'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_riesgo_bajo_duracion_cantidad|integer|
    #    |stg_riesgo_bajo_duracion_unidad|string|
    
    auxCol=esp_consolidado_corte['LOW_RISK_DURATION'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_riesgo_bajo_duracion_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_riesgo_bajo_duracion_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_recursos|string|
    #    |stg_actividad_sucesora|string|

    stg_consolidado_corte['stg_recursos']=esp_consolidado_corte['RESOURCES']
    stg_consolidado_corte['stg_actividad_sucesora']=esp_consolidado_corte['SUCCESORS']
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_impacto_buffer|integer|
    #    |stg_indicador_cantidad|integer|
    #    |stg_indicador_unidad|string|

    stg_consolidado_corte['stg_impacto_buffer']=pd.to_numeric(esp_consolidado_corte['PROJECT_BUFFER_IMPACT'])
    auxCol=esp_consolidado_corte['INDICATOR'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_indicador_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_indicador_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_porc_completitud|float|

    stg_consolidado_corte['stg_porc_completitud']=pd.to_numeric(esp_consolidado_corte['PCT_COMPLETED'])
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_ind_buffer|string|
    #    |stg_indice_buffer|string|

    stg_consolidado_corte['stg_ind_buffer']=esp_consolidado_corte['BUFFER']
    stg_consolidado_corte['stg_indice_buffer']=esp_consolidado_corte['BUFFER_INDEX']
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_duracion_critica_cantidad|integer|
    #    |stg_duracion_critica_unidad|string|
    
    auxCol=esp_consolidado_corte['CRITICAL_NUMBER'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_critica_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_critica_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_ind_tarea|string|
    #    |stg_ind_tarea_critica|string|
    
    stg_consolidado_corte['stg_ind_tarea']=esp_consolidado_corte['TASK']
    stg_consolidado_corte['stg_ind_tarea_critica']=esp_consolidado_corte['CRITICAL_TASK']
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_estado|integer|
    
    stg_consolidado_corte['stg_estado']=pd.to_numeric(esp_consolidado_corte['STATE'])
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_numero_esquema|string|

    stg_consolidado_corte['stg_numero_esquema']=esp_consolidado_corte['SCHEME_NUMBER']
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fecha_fin_planeada|date|
    
    auxCol=esp_consolidado_corte['LIKELY_FINISH_DATE'].str.split(" ", n=2,expand=True)
    stg_consolidado_corte['stg_fecha_fin_planeada']=pd.to_datetime(auxCol[1], dayfirst=True)
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_nombre_archivo|string|
    
    stg_consolidado_corte['stg_nombre_archivo']=esp_consolidado_corte['PROJECT']

    #Splitting Project Column into Five:
    #   * PROJECT_STATUS
    #   * PROJECT_NAME
    #   * PROJECT STAGE
    #   * PROJECT_COMPONENT
    #   * PROJECT_CUTOFF_DATE

    auxCol=esp_consolidado_corte["PROJECT"].str.split("_", n=5,expand=True)
    stg_consolidado_corte['stg_area_prodesa']=auxCol[0]
    stg_consolidado_corte['stg_codigo_proyecto']=auxCol[1]
    stg_consolidado_corte['stg_etapa_proyecto']=auxCol[2]
    stg_consolidado_corte['stg_programacion_proyecto']=auxCol[3]
    stg_consolidado_corte['stg_fecha_corte']=pd.to_datetime(auxCol[4], dayfirst=True)

    #Now continue with the column
    # |Column|Data Type|
    # |-----|----|
    # |stg_fecha_inicial|date|
    # |stg_fecha_fin|date|

    month_mapping={
        'enero':'1',
        'febrero':'2',
        'marzo':'3',
        'abril':'4',
        'mayo':'5',
        'junio':'6',
        'julio':'7',
        'agosto':'8',
        'septiembre':'9',
        'octubre':'10',
        'noviembre':'11',
        'diciembre':'12'
    }
    auxCol=esp_consolidado_corte["D_START"].str.split(" ", n=5,expand=True)
    auxCol['month']=auxCol[1].map(month_mapping)
    auxCol['date']=auxCol[0] + '-' + auxCol['month'] + '-' + auxCol[2]
    stg_consolidado_corte['stg_fecha_inicial']=pd.to_datetime(auxCol['date'], dayfirst=True)

    auxCol=esp_consolidado_corte["D_FINISH"].str.split(" ", n=5,expand=True)
    auxCol['month']=auxCol[1].map(month_mapping)
    auxCol['date']=auxCol[0] + '-' + auxCol['month'] + '-' + auxCol[2]
    stg_consolidado_corte['stg_fecha_fin']=pd.to_datetime(auxCol['date'], dayfirst=True)

    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_actividad_predecesora|string|
    #    |stg_notas|string|

    stg_consolidado_corte['stg_actividad_predecesora']=esp_consolidado_corte['PREDECESSOR']
    stg_consolidado_corte['stg_notas']=esp_consolidado_corte['NOTE']

    # Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_duracion_restante_cantidad|integer|
    #    |stg_duracion_restante_unidad|string|

    auxCol=esp_consolidado_corte['DURATION_RESTANTE'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_restante_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_restante_unidad']=auxCol[1]

    # Now continue with the column

    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fin_linea_base_estimado|date|

    auxCol=esp_consolidado_corte['FIN_LINEA_BASE_EST'].str.split(" ", n=2,expand=True)
    stg_consolidado_corte['stg_fin_linea_base_estimado']=pd.to_datetime(auxCol[1], dayfirst=True)

    # Indexing columns that will be used to generate reports, so it can be used faster.

    stg_consolidado_corte.reindex(columns=['stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual'])



    print(" -Staging Area ending...");

    return stg_consolidado_corte