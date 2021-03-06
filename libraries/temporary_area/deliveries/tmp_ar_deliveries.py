
import pandas as pd
import numpy as np

from libraries.settings import PRODESA_AREA_BUILDING

def tmp_ar_deliveries(stg_consolidado_corte, tbl_proyectos, current_bash):
    print("  *Deliveries Starting")
    
    deliveries_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fin_linea_base_estimado', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    deliveries_dataset['key']=deliveries_dataset['stg_codigo_proyecto']+'_'+deliveries_dataset['stg_etapa_proyecto']+'_'+deliveries_dataset['stg_programacion_proyecto']+'_'+deliveries_dataset['stg_nombre_actividad']
    deliveries_dataset=deliveries_dataset[deliveries_dataset['stg_area_prodesa']==PRODESA_AREA_BUILDING]
    #Fecha_final_actual en vez de fecha_fin_planeada
    cols_list = ['stg_nombre_actividad']
    search_values = ['ENTREGA']
    deliveries_dataset=deliveries_dataset[deliveries_dataset[cols_list].stack().str.contains('|'.join(search_values),case=False,na=False).groupby(level=0).any()]

    auxCol=deliveries_dataset.loc[:,('key','stg_fin_linea_base_estimado','stg_fecha_fin', 'stg_fecha_final_actual', 'stg_project_id')]
    tbl_reporte_por_entregas=auxCol.loc[:,('key','stg_fecha_final_actual', 'stg_fin_linea_base_estimado', 'stg_project_id')].rename(columns={'stg_fecha_final_actual':'trpe_entrega_real', 'stg_fin_linea_base_estimado':'trpe_entrega_programada'})
    tbl_reporte_por_entregas=tbl_reporte_por_entregas.sort_values(by=['key', 'stg_project_id'],ascending=True)
    tbl_reporte_por_entregas = tbl_reporte_por_entregas.groupby(by=["key"]).first().reset_index()

    tbl_reporte_por_entregas=pd.merge(tbl_reporte_por_entregas,deliveries_dataset.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_nombre_actividad')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_reporte_por_entregas = tbl_reporte_por_entregas.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto', 'stg_programacion_proyecto':'trpe_programacion', 'stg_nombre_actividad':'trpe_tarea_entrega'})
    tbl_reporte_por_entregas=pd.merge(tbl_reporte_por_entregas,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_reporte_por_entregas['stg_fecha_corte'] = deliveries_dataset['stg_fecha_corte'].iloc[0]
    tbl_reporte_por_entregas = tbl_reporte_por_entregas.rename(columns={'tpr_codigo_proyecto' : 'trpe_codigo_proyecto','tpr_regional' : 'trpe_regional','tpr_macroproyecto' : 'trpe_macroproyecto', 'stg_etapa_proyecto' : 'trpe_etapa', 'tpr_proyecto' : 'trpe_proyecto', 'stg_fecha_corte' : 'trpe_fecha_corte'})
    tbl_reporte_por_entregas['trpe_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tbl_reporte_por_entregas['trpe_lote_proceso']=current_bash

    tbl_reporte_por_entregas=tbl_reporte_por_entregas.reindex(columns=['trpe_regional',
                                                                    'trpe_codigo_proyecto',
                                                                    'trpe_macroproyecto',
                                                                    'trpe_proyecto',
                                                                    'trpe_programacion',
                                                                    'trpe_tarea_entrega',
                                                                    'trpe_etapa',
                                                                    'trpe_entrega_real',
                                                                    'trpe_entrega_programada',
                                                                    'trpe_fecha_corte',
                                                                    'trpe_fecha_proceso',
                                                                    'trpe_lote_proceso',
                                                                    ])

    print("  -Deliveries ending")
    return tbl_reporte_por_entregas