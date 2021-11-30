
from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER, TBL_PROYECTOS_CONSTRUCCION, TBL_VALORES_HITOS
import pandas as pd
import numpy as np

from google.cloud import bigquery

def tmp_ar_graphics(stg_consolidado_corte, 
                        tbl_proyectos, 
                        current_bash,
                        tmp_proyectos_construccion, 
                        tmp_proyectos_planeacion, 
                        tmp_proyectos_comercial,
                        building_report_excecution, 
                        planning_report_excecution,
                        commercial_report_excecution
                        ):

    print("  *Graphics Starting")


    #tbl_graficos_tiempo_avance_buffer = pd.DataFrame()
    column_names = ['tgabt_regional', 
                    'tgabt_codigo_proyecto', 
                    'tgabt_macroproyecto', 
                    'tgabt_proyecto', 
                    'tgabt_etapa',
                    'tgabt_programacion', 
                    'tgabt_avance_cc', 
                    'tgabt_consumo_buffer']
    tbl_graficos_tiempo_avance_buffer = pd.DataFrame(columns = column_names)

    if planning_report_excecution == True:
        auxCol = (tmp_proyectos_planeacion.loc[:,('tpp_regional', 
                                                    'tpp_codigo_proyecto', 
                                                    'tpp_macroproyecto', 
                                                    'tpp_proyecto', 
                                                    'tpp_etapa', 
                                                    'tpp_hito', 
                                                    'tpp_avance_cc', 
                                                    'tpp_consumo_buffer',
                                                    'tpp_fecha_corte'
                                                    )]).rename(columns={'tpp_regional': 'tgabt_regional', 
                                                                        'tpp_codigo_proyecto': 'tgabt_codigo_proyecto', 
                                                                        'tpp_macroproyecto': 'tgabt_macroproyecto', 
                                                                        'tpp_proyecto': 'tgabt_proyecto', 
                                                                        'tpp_etapa': 'tgabt_etapa',
                                                                        'tpp_hito':'tgabt_programacion', 
                                                                        'tpp_avance_cc':'tgabt_avance_cc', 
                                                                        'tpp_consumo_buffer':'tgabt_consumo_buffer',
                                                                        'tpp_fecha_corte':'tgabt_fecha_corte'
                                                                        })
        auxCol['tgabt_area_prodesa'] = 'PN'
        #print (auxCol)
        tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.append(auxCol)
    if commercial_report_excecution == True:
        auxCol2 = (tmp_proyectos_comercial.loc[:,('tpcm_regional', 
                                                    'tpcm_codigo_proyecto', 
                                                    'tpcm_macroproyecto', 
                                                    'tpcm_proyecto', 
                                                    'tpcm_etapa', 
                                                    'tpcm_programacion', 
                                                    'tpcm_avance_cc', 
                                                    'tpcm_consumo_buffer',
                                                    'tpcm_fecha_corte'
                                                    )]).rename(columns={'tpcm_regional': 'tgabt_regional', 
                                                                        'tpcm_codigo_proyecto': 'tgabt_codigo_proyecto', 
                                                                        'tpcm_macroproyecto': 'tgabt_macroproyecto', 
                                                                        'tpcm_proyecto': 'tgabt_proyecto', 
                                                                        'tpcm_etapa': 'tgabt_etapa',
                                                                        'tpcm_programacion':'tgabt_programacion', 
                                                                        'tpcm_avance_cc':'tgabt_avance_cc', 
                                                                        'tpcm_consumo_buffer':'tgabt_consumo_buffer',
                                                                        'tpcm_fecha_corte':'tgabt_fecha_corte'
                                                                        })
        auxCol2['tgabt_area_prodesa'] = 'CL'
        #print (auxCol2)
        tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.append(auxCol2)

    if building_report_excecution == True:
        auxCol3 = (tmp_proyectos_construccion.loc[:,('tpc_regional', 
                                                    'tpc_codigo_proyecto', 
                                                    'tpc_macroproyecto', 
                                                    'tpc_proyecto', 
                                                    'tpc_etapa', 
                                                    'tpc_programacion', 
                                                    'tpc_avance_cc', 
                                                    'tpc_consumo_buffer',
                                                    'tpc_fecha_corte'
                                                    )]).rename(columns={'tpc_regional': 'tgabt_regional', 
                                                                        'tpc_codigo_proyecto': 'tgabt_codigo_proyecto', 
                                                                        'tpc_macroproyecto': 'tgabt_macroproyecto', 
                                                                        'tpc_proyecto': 'tgabt_proyecto', 
                                                                        'tpc_etapa': 'tgabt_etapa',
                                                                        'tpc_programacion':'tgabt_programacion', 
                                                                        'tpc_avance_cc':'tgabt_avance_cc', 
                                                                        'tpc_consumo_buffer':'tgabt_consumo_buffer',
                                                                        'tpc_fecha_corte':'tgabt_fecha_corte'
                                                                        })
        auxCol3['tgabt_area_prodesa'] = 'CS'
        #tbl_graficos_tiempo_avance_buffer += auxCol3
        #tbl_graficos_tiempo_avance_buffer=pd.concat(tbl_graficos_tiempo_avance_buffer, auxCol3)
        #print (auxCol3)
        tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.append(auxCol3)

    
    
    tbl_graficos_tiempo_avance_buffer['key']=tbl_graficos_tiempo_avance_buffer['tgabt_area_prodesa']+'_'+tbl_graficos_tiempo_avance_buffer['tgabt_codigo_proyecto']+'_'+ tbl_graficos_tiempo_avance_buffer['tgabt_etapa'] +'_'+tbl_graficos_tiempo_avance_buffer['tgabt_programacion']

    #############################################

    #Lets start by building the dataset to work, which includes those registers which have the word CS, CL and PN in their stg_area_prodesa column

    planning_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte', 'stg_notas', 'stg_duracion_restante_cantidad', 'stg_fecha_inicial')]
    
    #print(planning_dataset['stg_notas'].unique())

    client = bigquery.Client()

    query ="""
        SELECT tvh_sigla,
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_VALORES_HITOS + """`
            WHERE tvh_estado = TRUE
            order by tvh_sigla desc
        """

    #print(query)        
    milestones_set= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))['tvh_sigla'].unique()
    
    first_time_loop=True
    for milestone in milestones_set:
        planning_dataset_filtered_set = planning_dataset[planning_dataset['stg_notas'].str.contains(milestone)]
        planning_dataset_filtered_set['stg_notas']=milestone
        if first_time_loop:
            planning_dataset_filtered = planning_dataset_filtered_set
            first_time_loop = False
        else:
            planning_dataset_filtered = planning_dataset_filtered.append(planning_dataset_filtered_set, ignore_index=True)
    planning_dataset=planning_dataset_filtered
    
    #print(planning_dataset.head(30))
    #print(planning_dataset['stg_notas'].unique())
    planning_dataset['key']=planning_dataset['stg_area_prodesa']+'_'+planning_dataset['stg_codigo_proyecto']+'_'+planning_dataset['stg_etapa_proyecto']+'_'+planning_dataset['stg_notas']
    planning_dataset=planning_dataset[planning_dataset['stg_area_prodesa']=='PN']
    #planning_dataset=planning_dataset[planning_dataset['stg_notas']!='-']
    planning_dataset=planning_dataset.dropna(subset=['stg_notas'])
    
    #Then define the report DataSet `tmp_proyectos_planeacion`, by setting its first three columns: 
    #    |Column|Data Type|
    #    |-----|----|
    #    |key| string|
    #    |tpp_codigo_proyecto|string|
    #    |tpp_etapa|string|
    #    |tpp_hito|string|
    #planning_dataset= planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_inicial', 'stg_fecha_fin')]
    
    #Column tgabt_fecha_inicio_linea_base_cargue
    startDate=(planning_dataset.loc[:, ('key', 'stg_fecha_inicial')]).sort_values(by=['key',"stg_fecha_inicial"],ascending=True,)
    startDate=startDate.groupby(by=["key"]).first().reset_index()
    startDate = startDate.rename(columns={'stg_fecha_inicial': 'tgabt_fecha_inicio_linea_base_cargue'})

    #Column tgabt_fecha_fin_linea_base_buffer_cargue
    endBufferDate = planning_dataset.loc[:, ('key', 'stg_ind_buffer','stg_fecha_fin')]
    endBufferDate = endBufferDate[endBufferDate['stg_ind_buffer']=='Sí']
    endBufferDate = (endBufferDate.loc[:, ('key','stg_fecha_fin')]).sort_values(by=['key',"stg_fecha_fin"],ascending=False,)
    endBufferDate = endBufferDate.rename(columns={'stg_fecha_fin': 'tgabt_fecha_fin_buffer_linea_base_cargue'})

    
    #Column tgabt_fecha_fin_linea_base_cargue
    endDate = planning_dataset.loc[:, ('key', 'stg_duracion_cantidad' ,'stg_fecha_fin')]
    endDate = endDate[endDate['stg_duracion_cantidad']==0]
    endDate = (endDate.loc[:, ('key','stg_fecha_fin')]).sort_values(by=['key',"stg_fecha_fin"],ascending=False,)
    endDate = endDate.rename(columns={'stg_fecha_fin': 'tgabt_fecha_fin_linea_base_cargue'})

    
    ##########################################
    #Lets start by building the dataset to work, which includes those registers which have the word CS, CL and PN in their stg_area_prodesa column

    building_and_commercial_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte', 'stg_fecha_inicial')]
    building_and_commercial_dataset['key']=building_and_commercial_dataset['stg_area_prodesa']+'_'+building_and_commercial_dataset['stg_codigo_proyecto']+'_'+building_and_commercial_dataset['stg_etapa_proyecto']+'_'+building_and_commercial_dataset['stg_programacion_proyecto']
    list_of_areas = ["CS", "CL"]
    building_and_commercial_dataset=building_and_commercial_dataset[building_and_commercial_dataset['stg_area_prodesa'].isin(list_of_areas)]
    #print(building_and_commercial_dataset)

    #Column tgabt_fecha_inicio_linea_base_cargue
    auxCol4=(building_and_commercial_dataset.loc[:, ('key', 'stg_fecha_inicial')]).sort_values(by=['key',"stg_fecha_inicial"],ascending=True,)
    auxCol4=auxCol4.groupby(by=["key"]).first().reset_index()
    auxCol4 = auxCol4.rename(columns={'stg_fecha_inicial': 'tgabt_fecha_inicio_linea_base_cargue'})

    #Column tgabt_fecha_fin_linea_base_buffer_cargue
    auxCol5 = building_and_commercial_dataset.loc[:, ('key', 'stg_ind_buffer','stg_fecha_fin')]
    auxCol5 = auxCol5[auxCol5['stg_ind_buffer']=='Sí']
    auxCol5 = (auxCol5.loc[:, ('key','stg_fecha_fin')]).sort_values(by=['key',"stg_fecha_fin"],ascending=False,)
    auxCol5 = auxCol5.rename(columns={'stg_fecha_fin': 'tgabt_fecha_fin_buffer_linea_base_cargue'})

    #Column tgabt_fecha_fin_linea_base_cargue
    auxCol6 = building_and_commercial_dataset.loc[:, ('key', 'stg_duracion_cantidad' ,'stg_fecha_fin')]
    auxCol6 = auxCol6[auxCol6['stg_duracion_cantidad']==0]
    auxCol6 = (auxCol6.loc[:, ('key','stg_fecha_fin')]).sort_values(by=['key',"stg_fecha_fin"],ascending=False,)
    auxCol6 = auxCol6.rename(columns={'stg_fecha_fin': 'tgabt_fecha_fin_linea_base_cargue'})

    #Appending dates calculated for commercial and construction reports
    startDate = startDate.append(auxCol4)
    endBufferDate = endBufferDate.append(auxCol5)
    endDate = endDate.append(auxCol6)
    
    #Merge
    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,startDate, on='key', how="left",)
    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,endBufferDate, on='key', how="left",)
    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,endDate, on='key', how="left",)

    #print (tbl_graficos_tiempo_avance_buffer)

    ###########################################################################################

    #Column tgabt_fecha_inicio_linea_base, tgabt_fecha_fin_linea_base, tgabt_fecha_fin_buffer_linea_base
    client = bigquery.Client()
    project_codes=tbl_graficos_tiempo_avance_buffer.tgabt_codigo_proyecto.unique()
    text=""
    for project_code in project_codes:
        if text== "":
            text=text+"'"+project_code+"'"
        else:
            text=text+", '"+project_code+"'"
    query ="""
        SELECT distinct CONCAT(tgabt_area_prodesa, '_', tt.tgabt_codigo_proyecto, '_', tt.tgabt_etapa, '_',tgabt_programacion) key, tgabt_fecha_inicio_linea_base, tgabt_fecha_fin_linea_base, tgabt_fecha_fin_buffer_linea_base
        FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER + """` tt 
        inner JOIN (SELECT CONCAT(tgabt_codigo_proyecto, '_', tgabt_etapa, '_',tgabt_programacion) key, MIN(tgabt_fecha_corte) AS MinDate
            FROM """ + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER + """
            WHERE tgabt_codigo_proyecto in ("""+text+""")
            GROUP BY key) groupedtt
        ON key = groupedtt.key 
        AND tt.tgabt_fecha_corte = groupedtt.MinDate 
        """

    #print(query)
    auxCol= client.query(query).result().to_dataframe(create_bqstorage_client=True,) 
    #print(auxCol.columns)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    #print(auxCol.head(5))
    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,auxCol.loc[:, ('tgabt_fecha_inicio_linea_base', 'tgabt_fecha_fin_linea_base', 'tgabt_fecha_fin_buffer_linea_base', 'key')], on='key', how="left",)
    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tbl_graficos_tiempo_avance_buffer['tgabt_lote_proceso']=current_bash

    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base_cargue'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base'])).astype('datetime64[ns]')
    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base_cargue'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base'])).astype('datetime64[ns]')
    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base_cargue'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base'])).astype('datetime64[ns]')
    
    
    #print (tbl_graficos_tiempo_avance_buffer.columns)
    tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.reindex(columns=['tgabt_area_prodesa',
                                                            'tgabt_regional',
                                                            'tgabt_codigo_proyecto',
                                                            'tgabt_macroproyecto',
                                                            'tgabt_proyecto',
                                                            'tgabt_etapa',
                                                            'tgabt_programacion',
                                                            'tgabt_avance_cc',
                                                            'tgabt_consumo_buffer',
                                                            'tgabt_fecha_inicio_linea_base_total',
                                                            'tgabt_fecha_fin_linea_base_total',
                                                            'tgabt_fecha_fin_buffer_linea_base_total',
                                                            'tgabt_fecha_corte',
                                                            'tgabt_fecha_proceso',
                                                            'tgabt_lote_proceso'])
    
    tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.rename(columns={'tgabt_fecha_inicio_linea_base_total':'tgabt_fecha_inicio_linea_base', 'tgabt_fecha_fin_linea_base_total':'tgabt_fecha_fin_linea_base', 'tgabt_fecha_fin_buffer_linea_base_total':'tgabt_fecha_fin_buffer_linea_base'})
    #tbl_graficos_tiempo_avance_buffer.rename(columns={'stg_fecha_corte': 'tgabt_fecha_corte', 'stg_area_prodesa': 'tgabt_area_prodesa', 'stg_codigo_proyecto': 'tgabt_codigo_proyecto', 'stg_etapa_proyecto': 'tgabt_etapa', 'stg_programacion_proyecto': 'tgabt_programacion'})
    tbl_graficos_tiempo_avance_buffer.dropna(inplace=True)

    #print (tbl_graficos_tiempo_avance_buffer.head(30))


    print("  -Graphics ending")
    return tbl_graficos_tiempo_avance_buffer