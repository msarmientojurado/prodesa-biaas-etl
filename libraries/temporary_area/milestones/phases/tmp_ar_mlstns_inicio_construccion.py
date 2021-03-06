
import pandas as pd
import numpy as np

def tmp_ar_mlstns_inicio_construccion(milestones_dataset, tbl_proyectos, current_bash):

    print("   *Inicio Construccion Starting")

    #start_building=milestones_dataset[milestones_dataset['stg_programacion_proyecto'] == "PL"]
    start_building=milestones_dataset
    
    tbl_inicio_construccion=pd.DataFrame()

    auxCol=start_building[start_building['stg_nombre_actividad'] == "1.INICIO DE CONSTRUCCION"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_inicio_construccion_programado', 'total':'tic_inicio_construccion_proyectado'})

    auxCol=start_building[start_building['stg_nombre_actividad'] == "11.Compras y contrataciones 80% act"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_proc_contratacion_programado', 'total':'tic_proc_contratacion_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "10.Kit 2 Construcciones"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_entrega_kit2_programado', 'total':'tic_entrega_kit2_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "9.Kit 1 Construcciones"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_entrega_kit1_programado', 'total':'tic_entrega_kit1_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "8.Aprobacion de FIC en CIP"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_factib_inic_constru_programado', 'total':'tic_factib_inic_constru_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "Ppto Definitivo (Tipo FIC)"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_ppto_definitivo_programado', 'total':'tic_ppto_definitivo_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "7.Presupuesto SIPRO"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_ppto_sipro_programado', 'total':'tic_ppto_sipro_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "Entrega de documentos para elaboraci??n de presupuesto de inicio de construcci??n"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_docs_inic_constru_programado', 'total':'tic_docs_inic_constru_proyectado'}), on='key', how="outer",)

    auxCol=start_building[start_building['stg_nombre_actividad'] == "Dise??o para inicio de construcci??n"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tic_diseno_inic_constru_programado', 'total':'tic_diseno_inic_constru_proyectado'}), on='key', how="outer",)


    tbl_inicio_construccion['tic_dias_atraso']=(tbl_inicio_construccion['tic_inicio_construccion_programado']-tbl_inicio_construccion['tic_inicio_construccion_proyectado']).dt.days
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion,start_building.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_construccion = tbl_inicio_construccion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_construccion=pd.merge(tbl_inicio_construccion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_construccion['stg_fecha_corte'] = start_building['stg_fecha_corte'].iloc[0]
    tbl_inicio_construccion = tbl_inicio_construccion.rename(columns={'tpr_codigo_proyecto' : 'tic_codigo_proyecto','tpr_regional' : 'tic_regional','tpr_macroproyecto' : 'tic_macroproyecto', 'stg_etapa_proyecto' : 'tic_etapa', 'tpr_proyecto' : 'tic_proyecto', 'stg_fecha_corte' : 'tic_fecha_corte'})
    tbl_inicio_construccion['tic_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tbl_inicio_construccion['tic_lote_proceso']=current_bash

    tbl_inicio_construccion=tbl_inicio_construccion.reindex(columns=['tic_regional',
                                                                        'tic_codigo_proyecto',
                                                                        'tic_macroproyecto',
                                                                        'tic_proyecto',
                                                                        'tic_etapa',
                                                                        'tic_dias_atraso',
                                                                        'tic_inicio_construccion_proyectado',
                                                                        'tic_inicio_construccion_programado',
                                                                        'tic_proc_contratacion_proyectado',
                                                                        'tic_proc_contratacion_programado',
                                                                        'tic_entrega_kit2_proyectado',
                                                                        'tic_entrega_kit2_programado',
                                                                        'tic_entrega_kit1_proyectado',
                                                                        'tic_entrega_kit1_programado',
                                                                        'tic_factib_inic_constru_proyectado',
                                                                        'tic_factib_inic_constru_programado',
                                                                        'tic_ppto_definitivo_proyectado',
                                                                        'tic_ppto_definitivo_programado',
                                                                        'tic_ppto_sipro_proyectado',
                                                                        'tic_ppto_sipro_programado',
                                                                        'tic_docs_inic_constru_proyectado',
                                                                        'tic_docs_inic_constru_programado',
                                                                        'tic_diseno_inic_constru_proyectado',
                                                                        'tic_diseno_inic_constru_programado',
                                                                        'tic_fecha_corte',
                                                                        'tic_fecha_proceso',
                                                                        'tic_lote_proceso'])

    print("   -Inicio Construccion ending")

    return tbl_inicio_construccion