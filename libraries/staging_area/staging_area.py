
from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_AREA_PRODESA, TBL_MAPEO_PROGRAMACION
from libraries.staging_area.information_consistency import information_consistency
import pandas as pd
import numpy as np
from google.cloud import bigquery

def staging_area(esp_consolidado_corte):

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
    
    esp_consolidado_corte['ACTUAL_START_DATE']=esp_consolidado_corte['ACTUAL_START_DATE'].replace("NOD", "")
    esp_consolidado_corte['ACTUAL_FINISH_DATE']=esp_consolidado_corte['ACTUAL_FINISH_DATE'].replace("NOD", "")

    stg_consolidado_corte['stg_fecha_inicial_actual']=pd.to_datetime(esp_consolidado_corte['ACTUAL_START_DATE'], dayfirst=True)
    stg_consolidado_corte['stg_fecha_final_actual']=pd.to_datetime(esp_consolidado_corte['ACTUAL_FINISH_DATE'], dayfirst=True)
    
    #Now continue with the columns
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_duracion_real_cantidad|float|
    #    |stg_duracion_real_unidad|string|

    auxCol=esp_consolidado_corte['DURATION_REMAINED'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_real_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_real_unidad']=auxCol[1]
    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fecha_inicio_planeada|date|

    esp_consolidado_corte['LIKELY_START_DATE']=esp_consolidado_corte['LIKELY_START_DATE'].replace("NOD", "")
    stg_consolidado_corte['stg_fecha_inicio_planeada']=pd.to_datetime(esp_consolidado_corte['LIKELY_START_DATE'], dayfirst=True)
    
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
    
    esp_consolidado_corte['LIKELY_FINISH_DATE']=esp_consolidado_corte['LIKELY_FINISH_DATE'].replace("NOD", "")
    stg_consolidado_corte['stg_fecha_fin_planeada']=pd.to_datetime(esp_consolidado_corte['LIKELY_FINISH_DATE'], dayfirst=True)

    
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_nombre_archivo|string|
    
    stg_consolidado_corte['stg_nombre_archivo']=esp_consolidado_corte['PROJECT']

    #Splitting Project Column into Five:
    #   * PROJECT_STATUS
    #   * PROJECT_NAME
    #   * PROJECT_STAGE
    #   * PROJECT_COMPONENT_CODE
    #   * PROJECT_CUTOFF_DATE

    #df[['stg_area_prodesa', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_codigo_programacion_proyecto', 'stg_fecha_corte']]
    auxCol[['stg_area_prodesa', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_codigo_programacion_proyecto', 'stg_fecha_corte']]=esp_consolidado_corte["PROJECT"].str.split("_", n=5,expand=True)

    client = bigquery.Client()

    query ="""
        SELECT (tap_sigla_area) as stg_area_prodesa, UPPER(tap_nombre_area) as stg_nombre_area,
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_AREA_PRODESA + """`
            WHERE tap_estado = TRUE;
        """

    #print(query)        
    prodesa_areas= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))
    #print(pd.merge(auxCol.loc[:, ('stg_area_prodesa')],prodesa_areas, on = 'stg_area_prodesa', how ="left"))
    stg_consolidado_corte['stg_area_prodesa']=(pd.merge(auxCol.loc[:, ('stg_area_prodesa')],prodesa_areas, on = 'stg_area_prodesa', how ="left"))['stg_nombre_area']
    stg_consolidado_corte['stg_codigo_proyecto']=auxCol['stg_codigo_proyecto']
    stg_consolidado_corte['stg_etapa_proyecto']=auxCol['stg_etapa_proyecto'].str.replace("ET","Etapa ")
    stg_consolidado_corte['stg_codigo_programacion_proyecto']=auxCol['stg_codigo_programacion_proyecto']
    stg_consolidado_corte['stg_fecha_corte']=pd.to_datetime(auxCol['stg_fecha_corte'], dayfirst=True)

    #print(stg_consolidado_corte['stg_area_prodesa'])
    #Now continue with the column
    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_programacion_proyecto|string|
    #
    #Which consists of a mapping process: It is risky because mapping process is not one on one. Customer approved the process


    client = bigquery.Client()

    query ="""
        SELECT tmp_codigo, tmp_nombre
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_MAPEO_PROGRAMACION + """`
            WHERE tmp_estado = TRUE
            order by length(tmp_codigo) desc
        """

    #print(query)        
    schedules= client.query(query).result().to_dataframe(create_bqstorage_client=True,)
    schedules=schedules.rename(columns={'tmp_codigo': 'stg_programacion_proyecto', 
                                        'tmp_nombre': 'stg_programacion_proyecto_mostrar'
                                        })

    #print(schedules)

    #stg_consolidado_corte['stg_programacion_proyecto_mostrar'] = stg_consolidado_corte['stg_programacion_proyecto']
    res = pd.DataFrame()
    res['stg_programacion_proyecto_mostrar']= stg_consolidado_corte['stg_codigo_programacion_proyecto']
    res['origin']=stg_consolidado_corte['stg_codigo_programacion_proyecto']
    for index, schedule in schedules.iterrows():
        res['rows_to_edit']=res['origin'].str.contains(schedule['stg_programacion_proyecto'])
        res['result'] = res['origin'].str.replace(schedule['stg_programacion_proyecto'],schedule['stg_programacion_proyecto_mostrar'])
        res['origin'] = res['origin'].str.replace(schedule['stg_programacion_proyecto'],'-')
        res['stg_programacion_proyecto_mostrar'] = np.where(res['rows_to_edit']==True,res['result'],res['stg_programacion_proyecto_mostrar'])
    
    stg_consolidado_corte['stg_programacion_proyecto'] = res['stg_programacion_proyecto_mostrar']


    ######################################


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

    auxCol=esp_consolidado_corte['DURACION_RESTANTE'].str.split(" ", n=2,expand=True)
    auxCol[0] = auxCol[0].str.replace(',','.')
    stg_consolidado_corte['stg_duracion_restante_cantidad']=pd.to_numeric(auxCol[0])
    stg_consolidado_corte['stg_duracion_restante_unidad']=auxCol[1]

    # Now continue with the column

    #    |Column|Data Type|
    #    |-----|----|
    #    |stg_fin_linea_base_estimado|date|

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

    esp_consolidado_corte['FIN_LINEA_BASE_EST']=esp_consolidado_corte['FIN_LINEA_BASE_EST'].replace("NOD", "")
    #stg_consolidado_corte['stg_fin_linea_base_estimado']=pd.to_datetime(esp_consolidado_corte['FIN_LINEA_BASE_EST'], dayfirst=True)
    auxCol=esp_consolidado_corte["FIN_LINEA_BASE_EST"].str.split(" ", n=5,expand=True)
    try:
        auxCol['month']=auxCol[1].map(month_mapping)
        auxCol['date']=auxCol[0] + '-' + auxCol['month'] + '-' + auxCol[2]
        stg_consolidado_corte['stg_fin_linea_base_estimado']=pd.to_datetime(auxCol['date'], dayfirst=True)
    except:
        stg_consolidado_corte['stg_fin_linea_base_estimado']=np.nan
    # Indexing columns that will be used to generate reports, so it can be used faster.

    stg_consolidado_corte.reindex(columns=['stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual'])

    #stg_consolidado_corte, continue_process = information_consistency(stg_consolidado_corte)

    print(" -Staging Area ending...");

    return stg_consolidado_corte
