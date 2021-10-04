
import pandas as pd
import numpy as np

def tmp_ar_deliveries(stg_consolidado_corte, tbl_proyectos):
    print("  *Deliveries Starting")
    
    deliveries_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    deliveries_dataset['key']=deliveries_dataset['stg_codigo_proyecto']+'_'+deliveries_dataset['stg_etapa_proyecto']+'_'+deliveries_dataset['stg_programacion_proyecto']+'_'+deliveries_dataset['stg_nombre_actividad']
    deliveries_dataset=deliveries_dataset[deliveries_dataset['stg_area_prodesa']=='CS']

    cols_list = ['stg_nombre_actividad']
    search_values = ['ENTREGA']
    deliveries_dataset=deliveries_dataset[deliveries_dataset[cols_list].stack().str.contains('|'.join(search_values),case=False,na=False).any(level=0)]

    auxCol=deliveries_dataset.loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_reporte_por_entregas=auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'trpe_entrega_real', 'total':'trpe_entrega_programada'})
    tbl_reporte_por_entregas=tbl_reporte_por_entregas.sort_values(by=['key'],ascending=True)

    tbl_reporte_por_entregas=pd.merge(tbl_reporte_por_entregas,deliveries_dataset.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_nombre_actividad')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_reporte_por_entregas = tbl_reporte_por_entregas.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto', 'stg_programacion_proyecto':'trpe_programacion', 'stg_nombre_actividad':'trpe_tarea_entrega'})
    tbl_reporte_por_entregas=pd.merge(tbl_reporte_por_entregas,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_reporte_por_entregas['stg_fecha_corte'] = deliveries_dataset['stg_fecha_corte'].iloc[0]
    tbl_reporte_por_entregas = tbl_reporte_por_entregas.rename(columns={'tpr_codigo_proyecto' : 'trpe_codigo_proyecto','tpr_regional' : 'trpe_regional','tpr_macroproyecto' : 'trpe_macroproyecto', 'stg_etapa_proyecto' : 'trpe_etapa', 'tpr_proyecto' : 'trpe_proyecto', 'stg_fecha_corte' : 'trpe_fecha_corte'})
    tbl_reporte_por_entregas['trpe_fecha_proceso']=pd.to_datetime("today").astype('date')
    tbl_reporte_por_entregas['trpe_lote_proceso']=1

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
    return