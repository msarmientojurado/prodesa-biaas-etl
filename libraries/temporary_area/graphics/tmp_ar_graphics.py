
from libraries.settings import BIGQUERY_ENVIRONMENT_NAME, TBL_GRAFICOS_TIEMPO_AVANCE_BUFFER, TBL_PROYECTOS_CONSTRUCCION
import pandas as pd
import numpy as np

from google.cloud import bigquery

def tmp_ar_graphics(stg_consolidado_corte, tbl_proyectos, current_bash):

    print("  *Graphics Starting")

    #Lets start by building the dataset to work, which includes those registers which have the word CS, CL and PN in their stg_area_prodesa column

    graphics_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte', 'stg_fecha_inicial')]
    graphics_dataset['key']=graphics_dataset['stg_codigo_proyecto']+'_'+graphics_dataset['stg_etapa_proyecto']+'_'+graphics_dataset['stg_programacion_proyecto']
    list_of_areas = ["CS", "CL", "PN"]
    graphics_dataset=graphics_dataset[graphics_dataset['stg_area_prodesa'].isin(list_of_areas)]

    #Then define the report DataSet `tbl_graficos_tiempo_avance_buffer`, by setting its first three columns: 

    #|Column|Data Type|
    #|-----|----|
    #|key| string|
    #|tgabt_area_prodesa|string|
    #|tgabt_codigo_proyecto|string|
    #|tgabt_etapa|string|
    #|tgabt_programacion|string|

    tbl_graficos_tiempo_avance_buffer= graphics_dataset.loc[:, ('key', 'stg_fecha_corte', 'stg_area_prodesa', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_fecha_inicial', 'stg_fecha_fin','stg_fecha_fin_planeada')]
    tbl_graficos_tiempo_avance_buffer = tbl_graficos_tiempo_avance_buffer.rename(columns={'stg_fecha_corte': 'tgabt_fecha_corte', 'stg_area_prodesa': 'tgabt_area_prodesa', 'stg_codigo_proyecto': 'tgabt_codigo_proyecto', 'stg_etapa_proyecto': 'tgabt_etapa', 'stg_programacion_proyecto': 'tgabt_programacion'})
    tbl_graficos_tiempo_avance_buffer=tbl_graficos_tiempo_avance_buffer.groupby(by=["key"]).first().reset_index()

    #Column 'tpc_avance_cc'

    #Lets start finding `max(stg_indicador_cantidad)`

    auxCol=graphics_dataset.loc[:, ('key', 'stg_indicador_cantidad')]
    auxCol=auxCol.dropna(subset=['stg_indicador_cantidad'], axis=0, inplace=False)
    auxCol.sort_values(by=['key',"stg_indicador_cantidad"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Find `max(stg_duracion_critica_cantidad)`

    auxCol2=graphics_dataset.loc[:, ('key', 'stg_duracion_critica_cantidad')]
    auxCol2=auxCol2.dropna(subset=['stg_duracion_critica_cantidad'], axis=0, inplace=False)
    auxCol2.sort_values(by=['key',"stg_duracion_critica_cantidad"],ascending=False, inplace=True)
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Lets calculate `% Avance CC`

    auxCol= pd.merge(auxCol,auxCol2, on='key', how="inner",)
    auxCol['tgabt_avance_cc']=1-(auxCol['stg_indicador_cantidad'].astype(float)/auxCol['stg_duracion_critica_cantidad'].astype(float))

    #Adding `tpc_avance_cc` column to the report

    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,auxCol.loc[:,('key', 'tgabt_avance_cc')], on='key', how="left",)

    #Column 'tpc_consumo_buffer'

    auxCol=graphics_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

    #Lets start by calculating the variable "Tamano Buffer" for each project, by following the steps below:
    #    * Filter the dataset by the column `stg_ind_buffer` equals `Sí`
    #    * Group the result Data Set by the column `key`
    #    * Thake the value of the column `stg_duracion_cantidad`: this is the value of the variable `Tamano Buffer`

    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

    #Lets continue calculating the variable `Fin Programada`for each project, by following the steps below:

    #    * Filter the dataset by the column stg_ind_buffer equals Sí
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `stg_fecha_fin`: this is the value of the variable `FinProgramada`

    auxCol2=graphics_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
    auxCol2=auxCol2[auxCol2['stg_ind_buffer']=='Sí']
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Finally calculate the variable `Fin Proyectada`for each project, by following the steps below:

    #    * Sort registers by the `stg_project_id` column
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `fin_proyectada` if it exists, otherwise, take the value in the column `stg_fecha_final_actual`: this is the value of the variable `FinProyectada`

    auxCol3=graphics_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    auxCol3=auxCol3[auxCol3['stg_ind_buffer']=='No']
    auxCol3.sort_values(by=['key',"stg_project_id"],ascending=False, inplace=True)
    auxCol3=auxCol3.groupby(by=["key"]).first().reset_index()
    auxCol3['fin_proyectada']=np.where(auxCol3['stg_fecha_fin_planeada'].isna(), auxCol3['stg_fecha_final_actual'], auxCol3['stg_fecha_fin_planeada'])

    #Now lets calculate `%ConsumoBuffer` Based on the given formula

    auxCol=pd.merge(auxCol,auxCol2.loc[:, ('key', 'stg_fecha_fin')], on='key', how="left",)
    auxCol=pd.merge(auxCol,auxCol3.loc[:, ('key', 'fin_proyectada')], on='key', how="left",)

    auxCol['delta_days']=(auxCol['stg_fecha_fin']-auxCol['fin_proyectada']).dt.days
    auxCol['tgabt_consumo_buffer']=(auxCol['stg_duracion_cantidad']-(auxCol['delta_days']-(auxCol['delta_days']/4.5)))/auxCol['stg_duracion_cantidad']

    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,auxCol.loc[:, ('key', 'tgabt_consumo_buffer')], on='key', how="left",)

    tbl_graficos_tiempo_avance_buffer.tgabt_avance_cc.dropna(inplace=True)
    tbl_graficos_tiempo_avance_buffer['tgabt_avance_cc']=np.where(tbl_graficos_tiempo_avance_buffer['tgabt_avance_cc']<0,0,tbl_graficos_tiempo_avance_buffer['tgabt_avance_cc'])

    tbl_graficos_tiempo_avance_buffer = tbl_graficos_tiempo_avance_buffer.rename(columns={'tgabt_codigo_proyecto': 'tpr_codigo_proyecto'})
    #print (tbl_graficos_tiempo_avance_buffer.head(30))
    tbl_graficos_tiempo_avance_buffer=pd.merge(tbl_graficos_tiempo_avance_buffer,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_graficos_tiempo_avance_buffer = tbl_graficos_tiempo_avance_buffer.rename(columns={'tpr_codigo_proyecto' : 'tgabt_codigo_proyecto','tpr_regional' : 'tgabt_regional','tpr_macroproyecto' : 'tgabt_macroproyecto', 'tpr_proyecto' : 'tgabt_proyecto'})
    
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
        SELECT distinct CONCAT(tt.tgabt_codigo_proyecto, '_', tt.tgabt_etapa, '_',tgabt_programacion) key, tgabt_fecha_inicio_linea_base, tgabt_fecha_fin_linea_base, tgabt_fecha_fin_buffer_linea_base
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

    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['stg_fecha_inicial'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_inicio_linea_base'])).astype('datetime64[ns]')
    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['stg_fecha_fin_planeada'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_linea_base'])).astype('datetime64[ns]')
    tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base_total']=(np.where(tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base'].isna(),tbl_graficos_tiempo_avance_buffer['stg_fecha_fin'],tbl_graficos_tiempo_avance_buffer['tgabt_fecha_fin_buffer_linea_base'])).astype('datetime64[ns]')
    
    
    print (tbl_graficos_tiempo_avance_buffer.columns)
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