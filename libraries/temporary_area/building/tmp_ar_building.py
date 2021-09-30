
from libraries.settings_ import BIGQUERY_ENVIRONMENT_NAME, TBL_PROYECTOS_CONSTRUCCION
import pandas as pd
import numpy as np

from google.cloud import bigquery


def tmp_ar_building(stg_consolidado_corte, tbl_proyectos):

    print("  *Building Starting")

    #Lets start by building the dataset to work, which includes those registers which have the word CS in their stg_area_prodesa column

    construction_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    construction_dataset['key']=construction_dataset['stg_codigo_proyecto']+'_'+construction_dataset['stg_etapa_proyecto']+'_'+construction_dataset['stg_programacion_proyecto']
    construction_dataset=construction_dataset[construction_dataset['stg_area_prodesa']=='CS']

    #Then define the report DataSet `tmp_proyectos_construccion`, by setting its first three columns: 

    #|Column|Data Type|
    #|-----|----|
    #|key| string|
    #|tpc_codigo_proyecto|string|
    #|tpc_etapa|string|
    #|tpc_programacion|string|

    tmp_proyectos_construccion= construction_dataset.loc[:, ('key', 'stg_fecha_corte', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto')]
    tmp_proyectos_construccion.rename(columns={'stg_codigo_proyecto': 'tpc_codigo_proyecto', 'stg_etapa_proyecto': 'tpc_etapa', 'stg_programacion_proyecto': 'tpc_programacion'})
    tmp_proyectos_construccion=tmp_proyectos_construccion.groupby(by=["key"]).first().reset_index()
    
    #Column 'tpc_tarea_consume_buffer'
    #   *   Step 1: Filter by column 'stg_ind_tarea', selecting those equals to 'Sí'. 
    #   *   Step 2: Order by column 'stg_fecha_inicio_planeada' ascending
    #   *   Step 3: Take the first item
    
    auxCol=construction_dataset.loc[:, ('key', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_nombre_actividad')]
    auxCol=auxCol[auxCol['stg_ind_tarea']=='Sí']
    auxCol=auxCol.sort_values(by=['stg_fecha_inicio_planeada'],ascending=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Adding `tpc_tarea_consume_buffer` column to the report

    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:,('key', 'stg_nombre_actividad')], on='key', how="left",)
    tmp_proyectos_construccion=tmp_proyectos_construccion.rename(columns={'stg_nombre_actividad':'tpc_tarea_consume_buffer'})

    #Column 'tpc_avance_cc'

    #Lets start finding `max(stg_indicador_cantidad)`

    auxCol=construction_dataset.loc[:, ('key', 'stg_indicador_cantidad')]
    auxCol=auxCol.dropna(subset=['stg_indicador_cantidad'], axis=0, inplace=False)
    auxCol.sort_values(by=['key',"stg_indicador_cantidad"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Find `max(stg_duracion_critica_cantidad)`

    auxCol2=construction_dataset.loc[:, ('key', 'stg_duracion_critica_cantidad')]
    auxCol2=auxCol2.dropna(subset=['stg_duracion_critica_cantidad'], axis=0, inplace=False)
    auxCol2.sort_values(by=['key',"stg_duracion_critica_cantidad"],ascending=False, inplace=True)
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Lets calculate `% Avance CC`

    auxCol= pd.merge(auxCol,auxCol2, on='key', how="inner",)
    auxCol['tpc_avance_cc']=(1-(auxCol['stg_indicador_cantidad'].astype(float)/auxCol['stg_duracion_critica_cantidad'].astype(float)))*100

    #Adding `tpc_avance_cc` column to the report

    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:,('key', 'tpc_avance_cc')], on='key', how="left",)

    #Column 'tpc_consumo_buffer'

    auxCol=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

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

    auxCol2=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
    auxCol2=auxCol2[auxCol2['stg_ind_buffer']=='Sí']
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Finally calculate the variable `Fin Proyectada`for each project, by following the steps below:

    #    * Sort registers by the `stg_project_id` column
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `fin_proyectada` if it exists, otherwise, take the value in the column `stg_fecha_final_actual`: this is the value of the variable `FinProyectada`

    auxCol3=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    auxCol3=auxCol3[auxCol3['stg_ind_buffer']=='No']
    auxCol3.sort_values(by=['key',"stg_project_id"],ascending=False, inplace=True)
    auxCol3=auxCol3.groupby(by=["key"]).first().reset_index()
    auxCol3['fin_proyectada']=np.where(auxCol3['stg_fecha_fin_planeada'].isna(), auxCol3['stg_fecha_final_actual'], auxCol3['stg_fecha_fin_planeada'])

    #Now lets calculate `%ConsumoBuffer` Based on the given formula

    auxCol=pd.merge(auxCol,auxCol2.loc[:, ('key', 'stg_fecha_fin')], on='key', how="left",)
    auxCol=pd.merge(auxCol,auxCol3.loc[:, ('key', 'fin_proyectada')], on='key', how="left",)

    auxCol['delta_days']=(auxCol['stg_fecha_fin']-auxCol['fin_proyectada']).dt.days
    auxCol['tpc_consumo_buffer']=100*(auxCol['stg_duracion_cantidad']-(auxCol['delta_days']-(auxCol['delta_days']/4.5)))/auxCol['stg_duracion_cantidad']

    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('key', 'tpc_consumo_buffer')], on='key', how="left",)

    #Column `tpc_fin_proyectado_optimista`

    #Lets find the dates for the `tpc_fin_proyectado_optimista` column by followinfg the procedure bellow:
    #    * Filter registers with condition `stg_duracion_cantidad = 0`
    #    * Sort register descending by column `stg_fecha_fin_planeada`
    #    * Group the result Data Set by the column key

    auxCol=construction_dataset.loc[:, ('key', 'stg_duracion_cantidad', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    #auxCol=auxCol[auxCol['stg_duracion_cantidad']==0]
    auxCol.sort_values(by=['key','stg_fecha_fin_planeada'],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Attach column `tpc_fin_proyectado_optimista` to `tmp_proyectos_construccion` Dataset
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('total','key')], on='key', how="left",)
    tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'total': 'tpc_fin_proyectado_optimista'})

    #Column `tpc_fin_proyectado_pesimista`
    #Lets start by calculating `TamanoBuffer`
    auxCol=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]
    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

    #Now merge columns `tpc_avance_cc` y `tpc_fin_proyectado_optimista` 
    # from the `tmp_proyectos_construccion` DataSet into the `auxCol` 
    # in order to have the complete information to calculate the equation above

    auxCol=pd.merge(auxCol,tmp_proyectos_construccion.loc[:, ('tpc_avance_cc','tpc_fin_proyectado_optimista','key')], on='key', how="left",)

    #Proceed with calculations of the equation above

    auxCol['delta']=(auxCol['stg_duracion_cantidad']*(1-(auxCol['tpc_avance_cc']/100)))
    auxCol['delta_days'] = auxCol['delta'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))
    auxCol['tpc_fin_proyectado_pesimista']=auxCol['tpc_fin_proyectado_optimista']+auxCol['delta_days']

    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('tpc_fin_proyectado_pesimista','key')], on='key', how="left",)

    #Column `tpc_fin_programada`
    auxCol=construction_dataset.loc[:, ('key', 'stg_fecha_fin')]
    auxCol.sort_values(by=['key',"stg_fecha_fin"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('stg_fecha_fin','key')], on='key', how="left",)
    tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_fecha_fin': 'tpc_fin_programada'})

    #Column `tpc_dias_atraso`
    tmp_proyectos_construccion['tpc_dias_atraso']=(tmp_proyectos_construccion['tpc_fin_programada']-tmp_proyectos_construccion['tpc_fin_proyectado_optimista']).dt.days

    tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'tpr_codigo_proyecto' : 'tpc_codigo_proyecto','tpr_regional' : 'tpc_regional','tpr_macroproyecto' : 'tpc_macroproyecto', 'stg_etapa_proyecto' : 'tpc_etapa', 'stg_programacion_proyecto' : 'tpc_programacion', 'tpr_proyecto' : 'tpc_proyecto', 'stg_fecha_corte' : 'tpc_fecha_corte'})

    #------------------------------
    #Column tpc_ultimo_mes

    client = bigquery.Client()
    project_codes=tmp_proyectos_construccion.tpc_codigo_proyecto.unique()
    cut_date = pd.to_datetime(tmp_proyectos_construccion.tpc_fecha_corte.unique()[0])
    project_codes=""
    for project_code in project_codes:
        if project_codes== "":
            project_codes=project_codes+"'"+project_code+"'"
        else:
            project_codes=project_codes+", '"+project_code+"'"
        
    query ="""
        SELECT distinct CONCAT(tt.tpc_codigo_proyecto, '_', tt.tpc_etapa, '_',tpc_programacion) key, tt.tpc_fecha_corte, tt.tpc_avance_cc
        FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_CONSTRUCCION + """` tt 
        inner JOIN (SELECT CONCAT(tpc_codigo_proyecto, '_', tpc_etapa, '_',tpc_programacion) key, MAX(tpc_fecha_corte) AS MaxDate
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_CONSTRUCCION + """`
            WHERE tpc_codigo_proyecto in ("""+project_codes+""")
            and tpc_fecha_corte <= DATE_SUB(DATE '""" + cut_date.strftime("%Y-%m-%d") +"""', INTERVAL 4 WEEK) 
            GROUP BY key) groupedtt
        ON key = groupedtt.key 
        AND tt.tpc_fecha_corte = groupedtt.MaxDate
        order by key, tt.tpc_fecha_corte desc 
        """

    #print(query)
    auxCol = client.query(query)

    auxCol= (
        client.query(query)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    #print(auxCol.columns)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('tpc_avance_cc','key')].rename(columns={'tpc_avance_cc':'tpc_avance_ultimo_mes'}), on='key', how="left",)
    tmp_proyectos_construccion['tpc_ultimo_mes']= tmp_proyectos_construccion['tpc_avance_cc']-tmp_proyectos_construccion['tpc_avance_ultimo_mes']

    #-----------------------------
    #Column tpc_ultima_semana
    query ="""
        SELECT distinct CONCAT(tt.tpc_codigo_proyecto, '_', tt.tpc_etapa, '_',tpc_programacion) key, tt.tpc_fecha_corte, tt.tpc_avance_cc, tt.tpc_consumo_buffer
        FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_CONSTRUCCION + """` tt 
        inner JOIN (SELECT CONCAT(tpc_codigo_proyecto, '_', tpc_etapa, '_',tpc_programacion) key, MAX(tpc_fecha_corte) AS MaxDate
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_CONSTRUCCION + """`
            WHERE tpc_codigo_proyecto in ("""+project_codes+""")
            GROUP BY key) groupedtt
        ON key = groupedtt.key 
        AND tt.tpc_fecha_corte = groupedtt.MaxDate
        order by key, tt.tpc_fecha_corte desc 
        """

    #print(query)
    auxCol = client.query(query)

    auxCol= (
        client.query(query)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    #print(auxCol.columns)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    #print(auxCol.head(5))
    tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('tpc_consumo_buffer', 'tpc_avance_cc','key')].rename(columns={'tpc_avance_cc':'tpc_avance_ultima_semana', 'tpc_consumo_buffer' : 'tpc_consumo_buffer_ultima_semana'}), on='key', how="left",)
    tmp_proyectos_construccion['tpc_ultima_semana']= tmp_proyectos_construccion['tpc_avance_cc']-tmp_proyectos_construccion['tpc_avance_ultima_semana']

    #Column tpc_avance_comparativo_semana
    conditions = [(tmp_proyectos_construccion['tpc_avance_ultima_semana'] < tmp_proyectos_construccion['tpc_avance_cc']),
                (tmp_proyectos_construccion['tpc_avance_ultima_semana'] == tmp_proyectos_construccion['tpc_avance_cc']),
                (tmp_proyectos_construccion['tpc_avance_ultima_semana'] > tmp_proyectos_construccion['tpc_avance_cc'])]
    
    choices = [1,0,-1]

    tmp_proyectos_construccion['tpc_avance_comparativo_semana'] = np.select(conditions, choices, default= 0 )

    #Column tpc_consumo_buffer_comparativo
    conditions = [(tmp_proyectos_construccion['tpc_consumo_buffer_ultima_semana'] < tmp_proyectos_construccion['tpc_consumo_buffer']),
                (tmp_proyectos_construccion['tpc_consumo_buffer_ultima_semana'] == tmp_proyectos_construccion['tpc_consumo_buffer']),
                (tmp_proyectos_construccion['tpc_consumo_buffer_ultima_semana'] > tmp_proyectos_construccion['tpc_consumo_buffer'])]
    
    choices = [1,0,-1]

    tmp_proyectos_construccion['tpc_consumo_buffer_comparativo'] = np.select(conditions, choices, default= 0 )

    #------------------------------

    #tmp_proyectos_construccion['tpc_avance_comparativo_semana']=0
    #tmp_proyectos_construccion['tpc_consumo_buffer_comparativo']=0
    #tmp_proyectos_construccion['tpc_ultima_semana']=0
    #tmp_proyectos_construccion['tpc_ultimo_mes']=0
    tmp_proyectos_construccion['tpc_fecha_proceso']=pd.to_datetime("today")
    tmp_proyectos_construccion['tpc_lote_proceso']=1

    tmp_proyectos_construccion['tpc_tarea_consume_buffer']=np.where(tmp_proyectos_construccion['tpc_avance_cc']==100,"TERMINADO",tmp_proyectos_construccion['tpc_tarea_consume_buffer'])

    conditions = [(tmp_proyectos_construccion['tpc_consumo_buffer']) < (0.2*tmp_proyectos_construccion['tpc_avance_cc']+20),
                (0.8*tmp_proyectos_construccion['tpc_avance_cc']+20 > tmp_proyectos_construccion['tpc_consumo_buffer']) & (tmp_proyectos_construccion['tpc_consumo_buffer'] > 0.2*tmp_proyectos_construccion['tpc_avance_cc']+20),
                (tmp_proyectos_construccion['tpc_consumo_buffer'] > 0.8*tmp_proyectos_construccion['tpc_avance_cc']+20)]
    
    choices = [1,0,-1]

    tmp_proyectos_construccion['tpc_consumo_buffer_color'] = np.select(conditions, choices, default= 1 )

    tmp_proyectos_construccion=tmp_proyectos_construccion.reindex(columns=['tpc_regional',
                                                            'tpc_codigo_proyecto',
                                                            'tpc_macroproyecto',
                                                            'tpc_proyecto',
                                                            'tpc_etapa',
                                                            'tpc_programacion',
                                                            'tpc_tarea_consume_buffer',
                                                            'tpc_avance_cc',
                                                            'tpc_avance_comparativo_semana',
                                                            'tpc_consumo_buffer',
                                                            'tpc_consumo_buffer_color',
                                                            'tpc_consumo_buffer_comparativo',
                                                            'tpc_fin_proyectado_optimista',
                                                            'tpc_fin_proyectado_pesimista',
                                                            'tpc_fin_programada',
                                                            'tpc_dias_atraso',
                                                            'tpc_ultima_semana',
                                                            'tpc_ultimo_mes',
                                                            'tpc_fecha_corte',
                                                            'tpc_fecha_proceso',
                                                            'tpc_lote_proceso'])


    print("  -Building ending")
    return tmp_proyectos_construccion