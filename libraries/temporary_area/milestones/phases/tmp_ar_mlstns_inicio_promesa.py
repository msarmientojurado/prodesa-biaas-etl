
import pandas as pd
import numpy as np

def tmp_ar_mlstns_inicio_promesa(milestones_dataset, tbl_proyectos):

    print("   *Inicio Promesas Starting")

    start_promise=milestones_dataset[milestones_dataset['stg_programacion_proyecto'] == "PL"]
    tbl_inicio_promesa=pd.DataFrame()

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "1.FIN INICIO PROMESAS"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_inicio_promesas_programado', 'total':'tip_inicio_promesas_proyectado'})

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "2.Entrega kit promesas"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_ent_kit_prom_programado', 'total':'tip_ent_kit_prom_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "3.Permiso de ventas"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_permiso_ventas_programado', 'total':'tip_permiso_ventas_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "4.Minuta de Hipoteca Registrada"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_min_hipo_reg_programado', 'total':'tip_min_hipo_reg_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "5.Radicacion de Minuta de Hipoteca a Registro"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_rad_min_hipo_reg_programado', 'total':'tip_rad_min_hipo_reg_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "6.Credito constructor"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_cred_construct_programado', 'total':'tip_cred_construct_proyectado'}), on='key', how="outer",)

    auxCol= start_promise[start_promise['stg_nombre_actividad'] == "Linderos"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_linderos_programado', 'total':'tip_linderos_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "11.FAI"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_fai_programado', 'total':'tip_fai_proyectado'}), on='key', how="outer",)

    auxCol=start_promise[start_promise['stg_nombre_actividad'] == "12.Constitucion de la Urbanizacion"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin', 'stg_fecha_final_actual')]
    auxCol['total']=np.where(auxCol['stg_fecha_fin_planeada'].isna(),auxCol['stg_fecha_final_actual'],auxCol['stg_fecha_fin_planeada'])
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa, auxCol.loc[:,('key','stg_fecha_fin', 'total')].rename(columns={'stg_fecha_fin':'tip_constitut_urban_programado', 'total':'tip_constitut_urban_proyectado'}), on='key', how="outer",)

    tbl_inicio_promesa['tip_dias_atraso']=(tbl_inicio_promesa['tip_inicio_promesas_programado']-tbl_inicio_promesa['tip_inicio_promesas_proyectado']).dt.days
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa,start_promise.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_promesa = tbl_inicio_promesa.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_promesa=pd.merge(tbl_inicio_promesa,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_promesa['stg_fecha_corte'] = start_promise['stg_fecha_corte'].iloc[0]
    tbl_inicio_promesa = tbl_inicio_promesa.rename(columns={'tpr_codigo_proyecto' : 'tip_codigo_proyecto','tpr_regional' : 'tip_regional','tpr_macroproyecto' : 'tip_macroproyecto', 'stg_etapa_proyecto' : 'tip_etapa', 'tpr_proyecto' : 'tip_proyecto', 'stg_fecha_corte': 'tip_fecha_corte'})
    tbl_inicio_promesa['tip_fecha_proceso']=pd.to_datetime("today")
    tbl_inicio_promesa['tip_lote_proceso']=1

    tbl_inicio_promesa=tbl_inicio_promesa.reindex(columns=['tip_regional',
                                                            'tip_codigo_proyecto',
                                                            'tip_macroproyecto',
                                                            'tip_proyecto',
                                                            'tip_etapa',
                                                            'tip_dias_atraso',
                                                            'tip_inicio_promesas_proyectado',
                                                            'tip_inicio_promesas_programado',
                                                            'tip_ent_kit_prom_proyectado',
                                                            'tip_ent_kit_prom_programado',
                                                            'tip_permiso_ventas_proyectado',
                                                            'tip_permiso_ventas_programado',
                                                            'tip_min_hipo_reg_proyectado',
                                                            'tip_min_hipo_reg_programado',
                                                            'tip_rad_min_hipo_reg_proyectado',
                                                            'tip_rad_min_hipo_reg_programado',
                                                            'tip_cred_construct_proyectado',
                                                            'tip_cred_construct_programado',
                                                            'tip_linderos_proyectado',
                                                            'tip_linderos_programado',
                                                            'tip_fai_proyectado',
                                                            'tip_fai_programado',
                                                            'tip_constitut_urban_proyectado',
                                                            'tip_constitut_urban_programado',
                                                            'tip_fecha_corte',
                                                            'tip_fecha_proceso',
                                                            'tip_lote_proceso'])

    print("   -Inicio Promesas ending")

    return tbl_inicio_promesa