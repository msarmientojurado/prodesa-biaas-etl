import pandas as pd
import numpy as np

from google.cloud import bigquery
from libraries.settings import TBL_PROYECTOS_PLANEACION, BIGQUERY_ENVIRONMENT_NAME, TBL_VALORES_HITOS

def tmp_ar_planning(stg_consolidado_corte, tbl_proyectos, current_bash):
    print("  *Planning Starting")
    #Lets start by building the dataset to work, which includes those 
    # registers which have the word `PN` in their `stg_area_prodesa` 
    # column

    planning_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte', 'stg_notas')]
    planning_dataset['key']=planning_dataset['stg_codigo_proyecto']+'_'+planning_dataset['stg_etapa_proyecto']+'_'+planning_dataset['stg_notas']
    planning_dataset=planning_dataset[planning_dataset['stg_area_prodesa']=='PN']
    planning_dataset=planning_dataset[planning_dataset['stg_notas']!='-']
    planning_dataset=planning_dataset.dropna(subset=['stg_notas'])

    #Then define the report DataSet `tmp_proyectos_planeacion`, by setting its first three columns: 
    #    |Column|Data Type|
    #    |-----|----|
    #    |key| string|
    #    |tpp_codigo_proyecto|string|
    #    |tpp_etapa|string|
    #    |tpp_hito|string|
    tmp_proyectos_planeacion= planning_dataset.loc[:, ('key', 'stg_fecha_corte', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_notas')]
    tmp_proyectos_planeacion=tmp_proyectos_planeacion.rename(columns={'stg_codigo_proyecto': 'tpp_codigo_proyecto', 'stg_etapa_proyecto': 'tpp_etapa', 'stg_notas': 'tpp_hito', 'stg_fecha_corte':'tpp_fecha_corte'})
    tmp_proyectos_planeacion=tmp_proyectos_planeacion.groupby(by=["key"]).first().reset_index()

    auxCol=planning_dataset.loc[:, ('key', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_nombre_actividad')]
    auxCol=auxCol[auxCol['stg_ind_tarea']=='Sí']
    auxCol=auxCol.sort_values(by=['stg_fecha_inicio_planeada'],ascending=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:,('key', 'stg_nombre_actividad')], on='key', how="left",)
    tmp_proyectos_planeacion=tmp_proyectos_planeacion.rename(columns={'stg_nombre_actividad':'tpp_tarea_consume_buffer'})

    #Column 'tpc_avance_cc'
    #Lets start finding `max(stg_indicador_cantidad)`
    auxCol=planning_dataset.loc[:, ('key', 'stg_indicador_cantidad')]
    auxCol=auxCol.dropna(subset=['stg_indicador_cantidad'], axis=0, inplace=False)
    auxCol.sort_values(by=['key',"stg_indicador_cantidad"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Find max(stg_duracion_critica_cantidad)
    auxCol2=planning_dataset.loc[:, ('key', 'stg_duracion_critica_cantidad')]
    auxCol2=auxCol2.dropna(subset=['stg_duracion_critica_cantidad'], axis=0, inplace=False)
    auxCol2.sort_values(by=['key',"stg_duracion_critica_cantidad"],ascending=False, inplace=True)
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Lets calculate `% Avance CC`
    auxCol= pd.merge(auxCol,auxCol2, on='key', how="inner",)
    auxCol['tpp_avance_cc']=(1-(auxCol['stg_indicador_cantidad'].astype(float)/auxCol['stg_duracion_critica_cantidad'].astype(float)))*100

    #Adding `tpc_avance_cc` column to the report
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:,('key', 'tpp_avance_cc')], on='key', how="left",)

    #Column 'tpc_consumo_buffer'
    auxCol=planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

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
    auxCol2=planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
    auxCol2=auxCol2[auxCol2['stg_ind_buffer']=='Sí']
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Finally calculate the variable `Fin Proyectada`for each project, by following the steps below:
    #    * Sort registers by the `stg_project_id` column
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `fin_proyectada` if it exists, otherwise, take the value in the column `stg_fecha_final_actual`: this is the value of the variable `FinProyectada`
    auxCol3=planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    auxCol3=auxCol3[auxCol3['stg_ind_buffer']=='No']
    auxCol3.sort_values(by=['key',"stg_project_id"],ascending=False, inplace=True)
    auxCol3=auxCol3.groupby(by=["key"]).first().reset_index()
    auxCol3['fin_proyectada']=np.where(auxCol3['stg_fecha_fin_planeada'].isna(), auxCol3['stg_fecha_final_actual'], auxCol3['stg_fecha_fin_planeada'])

    #Now lets calculate `%ConsumoBuffer` Based on the given formula
    auxCol=pd.merge(auxCol,auxCol2.loc[:, ('key', 'stg_fecha_fin')], on='key', how="left",)
    auxCol=pd.merge(auxCol,auxCol3.loc[:, ('key', 'fin_proyectada')], on='key', how="left",)

    auxCol['delta_days']=(auxCol['stg_fecha_fin']-auxCol['fin_proyectada']).dt.days
    auxCol['tpp_consumo_buffer']=100*(auxCol['stg_duracion_cantidad']-(auxCol['delta_days']-(auxCol['delta_days']/4.5)))/auxCol['stg_duracion_cantidad']

    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('key', 'tpp_consumo_buffer')], on='key', how="left",)

    #Column `tpc_fin_proyectado_optimista`
    #Lets find the dates for the `tpc_fin_proyectado_optimista` column by followinfg the procedure bellow:
    #    * Filter registers with condition `stg_duracion_cantidad = 0`
    #    * Sort register descending by column `stg_fecha_fin_planeada`
    #    * Group the result Data Set by the column key
    auxCol=planning_dataset.loc[:, ('key', 'stg_duracion_cantidad', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    #auxCol=auxCol[auxCol['stg_duracion_cantidad']==0]
    auxCol.sort_values(by=['key',"stg_fecha_fin_planeada","stg_fecha_final_actual"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Attach column `tpc_fin_proyectado_optimista` to `tmp_proyectos_construccion` Dataset
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('total','key')], on='key', how="left",)
    tmp_proyectos_planeacion = tmp_proyectos_planeacion.rename(columns={'total': 'tpp_fin_proyectado_optimista'})

    #Column `tpc_fin_proyectado_pesimista`
    #Lets start by calculating `TamanoBuffer`
    auxCol=planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]
    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

    #Now merge columns `tpc_avance_cc` y `tpc_fin_proyectado_optimista` from the `tmp_proyectos_construccion` DataSet into the `auxCol` in order to have the complete information to calculate the equation above
    auxCol=pd.merge(auxCol,tmp_proyectos_planeacion.loc[:, ('tpp_avance_cc','tpp_fin_proyectado_optimista','key')], on='key', how="left",)

    #Proceed with calculations of the equation above
    auxCol['delta']=(auxCol['stg_duracion_cantidad']*(1-(auxCol['tpp_avance_cc']/100))).astype(int)
    auxCol['delta_days'] = auxCol['delta'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))
    auxCol['tpp_fin_proyectado_pesimista']=auxCol['tpp_fin_proyectado_optimista']+auxCol['delta_days']

    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('tpp_fin_proyectado_pesimista','key')], on='key', how="left",)

    #Column `tpc_fin_programada`
    
    auxCol=planning_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol.sort_values(by=['key',"stg_fecha_fin"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('stg_fecha_fin','key')], on='key', how="left",)
    tmp_proyectos_planeacion = tmp_proyectos_planeacion.rename(columns={'stg_fecha_fin': 'tpp_fin_programada'})

    #Column `tpc_dias_atraso`
    tmp_proyectos_planeacion['tpp_dias_atraso']=(tmp_proyectos_planeacion['tpp_fin_programada']-tmp_proyectos_planeacion['tpp_fin_proyectado_optimista']).dt.days

    tmp_proyectos_planeacion = tmp_proyectos_planeacion.rename(columns={'tpp_codigo_proyecto': 'tpr_codigo_proyecto'})
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tmp_proyectos_planeacion = tmp_proyectos_planeacion.rename(columns={'tpr_regional' : 'tpp_regional','tpr_macroproyecto' : 'tpp_macroproyecto', 'stg_etapa_proyecto' : 'tpp_etapa', 'tpr_proyecto' : 'tpp_proyecto', 'stg_fecha_corte' : 'tpp_fecha_corte', 'tpr_codigo_proyecto': 'tpp_codigo_proyecto'})

    #------------------------------
    #Column tpc_ultimo_mes

    client = bigquery.Client()
    project_codes=tmp_proyectos_planeacion.tpp_codigo_proyecto.unique()
    cut_date = pd.to_datetime(tmp_proyectos_planeacion.tpp_fecha_corte.unique()[0])
    text=""
    for project_code in project_codes:
        if text== "":
            text=text+"'"+project_code+"'"
        else:
            text=text+", '"+project_code+"'"
    
    #planning_dataset['key']=planning_dataset['stg_codigo_proyecto']+'_'+planning_dataset['stg_etapa_proyecto']+'_'+planning_dataset['stg_notas']

    query ="""
        SELECT distinct CONCAT(tt.tpp_codigo_proyecto, '_', tt.tpp_etapa, '_',tpp_hito) key, tt.tpp_fecha_corte, tt.tpp_avance_cc
        FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_PLANEACION + """` tt 
        inner JOIN (SELECT CONCAT(tpp_codigo_proyecto, '_', tpp_etapa, '_',tpp_hito) key, MAX(tpp_fecha_corte) AS MaxDate
            FROM """ + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_PLANEACION + """
            WHERE tpp_codigo_proyecto in ("""+text+""")
            and tpp_fecha_corte <= DATE_SUB(DATE '""" + cut_date.strftime("%Y-%m-%d") +"""', INTERVAL 4 WEEK) 
            GROUP BY key) groupedtt
        ON key = groupedtt.key 
        AND tt.tpp_fecha_corte = groupedtt.MaxDate
        order by key, tt.tpp_fecha_corte desc 
        """

    #print(query)
    auxCol=client.query(query).result().to_dataframe(create_bqstorage_client=True,)
    #print(auxCol.columns)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol['tpp_avance_cc']=auxCol['tpp_avance_cc']*100
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('tpp_avance_cc','key')].rename(columns={'tpp_avance_cc':'tpp_avance_ultimo_mes'}), on='key', how="left",)
    tmp_proyectos_planeacion['tpp_ultimo_mes']= tmp_proyectos_planeacion['tpp_avance_cc']-tmp_proyectos_planeacion['tpp_avance_ultimo_mes']

    #-----------------------------
    #Column tpc_ultima_semana
    query ="""
        SELECT distinct CONCAT(tt.tpp_codigo_proyecto, '_', tt.tpp_etapa, '_',tpp_hito) key, tt.tpp_fecha_corte, tt.tpp_avance_cc, tt.tpp_consumo_buffer
        FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_PLANEACION + """` tt 
        inner JOIN (SELECT CONCAT(tpp_codigo_proyecto, '_', tpp_etapa, '_',tpp_hito) key, MAX(tpp_fecha_corte) AS MaxDate
            FROM """ + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS_PLANEACION + """
            WHERE tpp_codigo_proyecto in ("""+text+""")
            and tpp_fecha_corte < DATE '""" + cut_date.strftime("%Y-%m-%d") +"""'
            GROUP BY key) groupedtt
        ON key = groupedtt.key 
        AND tt.tpp_fecha_corte = groupedtt.MaxDate
        order by key, tt.tpp_fecha_corte desc 
        """

    #print(query)
    auxCol= client.query(query).result().to_dataframe(create_bqstorage_client=True,) 
    #print(auxCol.columns)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    #print(auxCol.head(5))
    auxCol['tpp_consumo_buffer']=auxCol['tpp_consumo_buffer']*100
    auxCol['tpp_avance_cc']=auxCol['tpp_avance_cc']*100
    tmp_proyectos_planeacion=pd.merge(tmp_proyectos_planeacion,auxCol.loc[:, ('tpp_consumo_buffer', 'tpp_avance_cc','key')].rename(columns={'tpp_avance_cc':'tpp_avance_ultima_semana', 'tpp_consumo_buffer' : 'tpp_consumo_buffer_ultima_semana'}), on='key', how="left",)
    tmp_proyectos_planeacion['tpp_ultima_semana']= tmp_proyectos_planeacion['tpp_avance_cc']-tmp_proyectos_planeacion['tpp_avance_ultima_semana']

    #Column tpc_avance_comparativo_semana
    conditions = [(tmp_proyectos_planeacion['tpp_avance_ultima_semana'] < tmp_proyectos_planeacion['tpp_avance_cc']),
                (tmp_proyectos_planeacion['tpp_avance_ultima_semana'] == tmp_proyectos_planeacion['tpp_avance_cc']),
                (tmp_proyectos_planeacion['tpp_avance_ultima_semana'] > tmp_proyectos_planeacion['tpp_avance_cc'])]
    
    choices = [1,0,-1]

    tmp_proyectos_planeacion['tpp_avance_comparativo_semana'] = np.select(conditions, choices, default= 0 )

    #Column tpc_consumo_buffer_comparativo
    conditions = [(tmp_proyectos_planeacion['tpp_consumo_buffer_ultima_semana'] < tmp_proyectos_planeacion['tpp_consumo_buffer']),
                (tmp_proyectos_planeacion['tpp_consumo_buffer_ultima_semana'] == tmp_proyectos_planeacion['tpp_consumo_buffer']),
                (tmp_proyectos_planeacion['tpp_consumo_buffer_ultima_semana'] > tmp_proyectos_planeacion['tpp_consumo_buffer'])]
    
    choices = [1,0,-1]

    tmp_proyectos_planeacion['tpp_consumo_buffer_comparativo'] = np.select(conditions, choices, default= 0 )

    #------------------------------

    #tmp_proyectos_planeacion['tpp_avance_comparativo_semana']=0
    #tmp_proyectos_planeacion['tpp_consumo_buffer_comparativo']=0
    #tmp_proyectos_planeacion['tpp_ultima_semana']=0
    #tmp_proyectos_planeacion['tpp_ultimo_mes']=0
    tmp_proyectos_planeacion['tpp_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tmp_proyectos_planeacion['tpp_lote_proceso']=current_bash

    client = bigquery.Client()

    query ="""
        SELECT tvh_sigla,
            FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_VALORES_HITOS + """`
            WHERE tvh_estado = TRUE
        """

    #print(query)        
    planning_items= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))['tvh_sigla'].unique()

    #planning_items=['PL','IV','IP','GAS','AC','GASUE','ELS1','EL','EL…','ELS0','IP…','SPU','SP','IC','DC','IE','SPUE']
    tmp_proyectos_planeacion=tmp_proyectos_planeacion[tmp_proyectos_planeacion['tpp_hito'].isin(planning_items)]
    tmp_proyectos_planeacion['tpp_tarea_consume_buffer']=np.where(tmp_proyectos_planeacion['tpp_avance_cc']==100,"TERMINADO",tmp_proyectos_planeacion['tpp_tarea_consume_buffer'])

    conditions = [(tmp_proyectos_planeacion['tpp_consumo_buffer']) < (0.2*tmp_proyectos_planeacion['tpp_avance_cc']+20),
                (0.8*tmp_proyectos_planeacion['tpp_avance_cc']+20 > tmp_proyectos_planeacion['tpp_consumo_buffer']) & (tmp_proyectos_planeacion['tpp_consumo_buffer'] > 0.2*tmp_proyectos_planeacion['tpp_avance_cc']+20),
                (tmp_proyectos_planeacion['tpp_consumo_buffer'] > 0.8*tmp_proyectos_planeacion['tpp_avance_cc']+20)]
    
    choices = [1,0,-1]

    tmp_proyectos_planeacion['tpp_consumo_buffer_color'] = np.select(conditions, choices, default= 1 )

    tmp_proyectos_planeacion['tpp_avance_cc'] = tmp_proyectos_planeacion['tpp_avance_cc'].div(100)
    tmp_proyectos_planeacion['tpp_consumo_buffer'] = tmp_proyectos_planeacion['tpp_consumo_buffer'].div(100)
    tmp_proyectos_planeacion['tpp_ultima_semana'] = tmp_proyectos_planeacion['tpp_ultima_semana'].div(100)
    tmp_proyectos_planeacion['tpp_ultimo_mes'] = tmp_proyectos_planeacion['tpp_ultimo_mes'].div(100)

    tmp_proyectos_planeacion.tpp_avance_cc.dropna(inplace=True)
    tmp_proyectos_planeacion['tpp_avance_cc']=np.where(tmp_proyectos_planeacion['tpp_avance_cc']<0,0,tmp_proyectos_planeacion['tpp_avance_cc'])

    tmp_proyectos_planeacion=tmp_proyectos_planeacion.reindex(columns=['tpp_regional',
                                                            'tpp_codigo_proyecto',
                                                            'tpp_macroproyecto',
                                                            'tpp_proyecto',
                                                            'tpp_hito',
                                                            'tpp_etapa',
                                                            'tpp_tarea_consume_buffer',
                                                            'tpp_avance_cc',
                                                            'tpp_avance_comparativo_semana',
                                                            'tpp_consumo_buffer',
                                                            'tpp_consumo_buffer_color',
                                                            'tpp_consumo_buffer_comparativo',
                                                            'tpp_fin_proyectado_optimista',
                                                            'tpp_fin_proyectado_pesimista',
                                                            'tpp_fin_programada',
                                                            'tpp_dias_atraso',
                                                            'tpp_ultima_semana',
                                                            'tpp_ultimo_mes',
                                                            'tpp_fecha_corte',
                                                            'tpp_fecha_proceso',
                                                            'tpp_lote_proceso'])
    
    print("  *Planning ending")

    return tmp_proyectos_planeacion