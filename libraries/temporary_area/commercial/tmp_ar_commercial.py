import pandas as pd
import numpy as np

def tmp_ar_commercial(stg_consolidado_corte):
    print("  *Commercial Starting")
    #Lets start by building the dataset to work, which includes those registers which have the word `CL` in their `stg_area_prodesa` column

    commercial_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    commercial_dataset['key']=commercial_dataset['stg_codigo_proyecto']+'_'+commercial_dataset['stg_etapa_proyecto']+'_'+commercial_dataset['stg_programacion_proyecto']
    commercial_dataset=commercial_dataset[commercial_dataset['stg_area_prodesa']=='CL']

    #Then define the report DataSet `tmp_proyectos_comercial`, by setting its first three columns: 

    #    |Column|Data Type|
    #    |-----|----|
    #    |key| string|
    #    |tpcm_codigo_proyecto|string|
    #    |tpcm_etapa|string|
    #    |tpcm_programacion|string|
    tmp_proyectos_comercial= commercial_dataset.loc[:, ('key', 'stg_fecha_corte', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto')]
    tmp_proyectos_comercial.rename(columns={'stg_codigo_proyecto': 'tpcm_codigo_proyecto', 'stg_etapa_proyecto': 'tpcm_etapa', 'stg_programacion_proyecto': 'tpcm_programacion'})
    tmp_proyectos_comercial=tmp_proyectos_comercial.groupby(by=["key"]).first().reset_index()

    #Column 'tpcm_tarea_consume_buffer'
    #   *   Step 1: Filter by column 'stg_ind_tarea', selecting those equals to 'Sí'. 
    #   *   Step 2: Order by column 'stg_fecha_inicio_planeada' ascending
    #   *   Step 3: Take the first item
    auxCol=commercial_dataset.loc[:, ('key', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_nombre_actividad')]
    auxCol=auxCol[auxCol['stg_ind_tarea']=='Sí']
    auxCol=auxCol.sort_values(by=['stg_fecha_inicio_planeada'],ascending=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Adding `tpcm|_tarea_consume_buffer` column to the report
    tmp_proyectos_comercial=pd.merge(tmp_proyectos_comercial,auxCol.loc[:,('key', 'stg_nombre_actividad')], on='key', how="left",)
    tmp_proyectos_comercial=tmp_proyectos_comercial.rename(columns={'stg_nombre_actividad':'tpcm_tarea_consume_buffer'})

    #Column 'tpc_avance_cc'
    #Lets start finding max(stg_indicador_cantidad)
    auxCol=commercial_dataset.loc[:, ('key', 'stg_indicador_cantidad')]
    auxCol=auxCol.dropna(subset=['stg_indicador_cantidad'], axis=0, inplace=False)
    auxCol.sort_values(by=['key',"stg_indicador_cantidad"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Find max(stg_duracion_critica_cantidad)
    auxCol2=commercial_dataset.loc[:, ('key', 'stg_duracion_critica_cantidad')]
    auxCol2=auxCol2.dropna(subset=['stg_duracion_critica_cantidad'], axis=0, inplace=False)
    auxCol2.sort_values(by=['key',"stg_duracion_critica_cantidad"],ascending=False, inplace=True)
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Lets calculate `% Avance CC`
    auxCol= pd.merge(auxCol,auxCol2, on='key', how="inner",)
    auxCol['tpcm_avance_cc']=(1-(auxCol['stg_indicador_cantidad'].astype(float)/auxCol['stg_duracion_critica_cantidad'].astype(float)))*100

    #Adding `tpcm_avance_cc` column to the report
    tmp_proyectos_comercial=pd.merge(tmp_proyectos_comercial,auxCol.loc[:,('key', 'tpcm_avance_cc')], on='key', how="left",)

    #Column 'tpcm_consumo_buffer'
    auxCol=commercial_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

    #Lets start by calculating the variable "Tamano Buffer" for each project, by following the steps below:
    #    * Filter the dataset by the column `stg_ind_buffer` equals `Sí`
    #    * Group the result Data Set by the column `key`
    #    * Thake the value of the column `stg_duracion_cantidad`: this is the value of the variable `Tamano Buffer`
    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

    #Lets continue calculating the variable `Fin Programada`for each project, by following the steps below:
    #    * Filter the dataset by the column stg_ind_buffer equals `Sí`
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `stg_fecha_fin`: this is the value of the variable `FinProgramada`
    auxCol2=commercial_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
    auxCol2=auxCol2[auxCol2['stg_ind_buffer']=='Sí']
    auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

    #Finally calculate the variable `Fin Proyectada`for each project, by following the steps below:
    #    * Sort registers by the `stg_project_id` column
    #    * Group the result Data Set by the column key
    #    * Thake the value of the column `fin_proyectada` if it exists, otherwise, take the value in the column `stg_fecha_final_actual`: this is the value of the variable `FinProyectada`
    auxCol3=commercial_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
    auxCol3=auxCol3[auxCol3['stg_ind_buffer']=='No']
    auxCol3.sort_values(by=['key',"stg_project_id"],ascending=False, inplace=True)
    auxCol3=auxCol3.groupby(by=["key"]).first().reset_index()
    auxCol3['fin_proyectada']=np.where(auxCol3['stg_fecha_fin_planeada'].isna(), auxCol3['stg_fecha_final_actual'], auxCol3['stg_fecha_fin_planeada'])

    #Now lets calculate `%ConsumoBuffer` Based on the given formula
    auxCol=pd.merge(auxCol,auxCol2.loc[:, ('key', 'stg_fecha_fin')], on='key', how="left",)
    auxCol=pd.merge(auxCol,auxCol3.loc[:, ('key', 'fin_proyectada')], on='key', how="left",)

    auxCol['delta_days']=(auxCol['stg_fecha_fin']-auxCol['fin_proyectada']).dt.days
    auxCol['tpcm_consumo_buffer']=100*(auxCol['stg_duracion_cantidad']-(auxCol['delta_days']-(auxCol['delta_days']/4.5)))/auxCol['stg_duracion_cantidad']

    tmp_proyectos_comercial=pd.merge(tmp_proyectos_comercial,auxCol.loc[:, ('key', 'tpcm_consumo_buffer')], on='key', how="left",)

    #Column `tpc_fin_proyectado_optimista`
    #Lets find the dates for the `tpcm_fin_proyectado_optimista` column by followinfg the procedure bellow:
    #    * Filter registers with condition `stg_duracion_cantidad = 0`
    #    * Sort register descending by column `stg_fecha_fin_planeada`
    #    * Group the result Data Set by the column key
    auxCol=commercial_dataset.loc[:, ('key', 'stg_duracion_cantidad', 'stg_fecha_fin_planeada')]
    auxCol=auxCol[auxCol['stg_duracion_cantidad']==0]
    auxCol.sort_values(by=['key',"stg_fecha_fin_planeada"],ascending=False, inplace=True)
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()

    #Now Attach column `tpc_fin_proyectado_optimista` to `tmp_proyectos_construccion` Dataset
    tmp_proyectos_comercial=pd.merge(tmp_proyectos_comercial,auxCol.loc[:, ('stg_fecha_fin_planeada','key')], on='key', how="left",)
    tmp_proyectos_comercial = tmp_proyectos_comercial.rename(columns={'stg_fecha_fin_planeada': 'tpcm_fin_proyectado_optimista'})

    #Column `tpc_fin_proyectado_pesimista`
    #Lets start by calculating `TamanoBuffer`
    auxCol=commercial_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

    auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
    auxCol=auxCol.groupby(by=["key"]).first().reset_index()
    auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

    #Now merge columns `tpc_avance_cc` y `tpc_fin_proyectado_optimista` from the `tmp_proyectos_construccion` DataSet into the `auxCol` in order to have the complete information to calculate the equation above
    auxCol=pd.merge(auxCol,tmp_proyectos_comercial.loc[:, ('tpcm_avance_cc','tpcm_fin_proyectado_optimista','key')], on='key', how="left",)

    #Proceed with calculations of the equation above
    auxCol['delta']=(auxCol['stg_duracion_cantidad']*(1-(auxCol['tpcm_avance_cc']/100)))
    auxCol['delta_days'] = auxCol['delta'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))

    #TODO Finish calculus
    # auxCol['tpcm_fin_proyectado_pesimista']=auxCol['tpcm_fin_proyectado_optimista']+auxCol['delta_days']

    print("  *Commercial ending")

    return tmp_proyectos_comercial

