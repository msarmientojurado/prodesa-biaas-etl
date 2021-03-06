
import pandas as pd
import numpy as np

def tmp_ar_mlstns_inicio_escrituracion(milestones_dataset,tbl_proyectos, current_bash):

    print("   *Inicio Escrituracion Starting")

    #start_registration=milestones_dataset[milestones_dataset['stg_programacion_proyecto'] == "PL"]
    start_registration=milestones_dataset
    
    tbl_inicio_escrituracion=pd.DataFrame()

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "1.INICIO ESCRITURACION"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion= auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_inicio_escrituracion_programado', 'total':'tie_inicio_escrituracion_proyectado'})

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "2.Poder firma de escrituras"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_poder_fiduciaria_programado', 'total':'tie_poder_fiduciaria_proyectado'}), on='key', how="outer",)

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "3.Salida de Registro RPH y Entrega de Folios"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_salida_rph_programado', 'total':'tie_salida_rph_proyectado'}), on='key', how="outer",)

    auxCol=start_registration[(start_registration['stg_nombre_actividad'] == "4.Cierre y numeracion de escritura de RPH") | (start_registration['stg_nombre_actividad'] == "4.Cierre y numeraci??n de escritura de RPH")].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_cierre_rph_programado', 'total':'tie_cierre_rph_proyectado'}), on='key', how="outer",)

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "5.Ejecutoria y entrega de licencia PH"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_licencia_ph_programado', 'total':'tie_licencia_ph_proyectado'}), on='key', how="outer",)

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "6.Modificaci??n LC"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_modificacion_lc_programado', 'total':'tie_modificacion_lc_proyectado'}), on='key', how="outer",)

    auxCol=start_registration[start_registration['stg_nombre_actividad'] == "7.Radicacion Modificaci??n Licencia de Construcci??n"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tie_radic_modif_lc_programado', 'total':'tie_radic_modif_lc_proyectado'}), on='key', how="outer",)


    tbl_inicio_escrituracion['tie_dias_atraso']=(tbl_inicio_escrituracion['tie_inicio_escrituracion_programado']-tbl_inicio_escrituracion['tie_inicio_escrituracion_proyectado']).dt.days
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion,start_registration.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_escrituracion = tbl_inicio_escrituracion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_escrituracion['stg_fecha_corte'] = start_registration['stg_fecha_corte'].iloc[0]
    tbl_inicio_escrituracion = tbl_inicio_escrituracion.rename(columns={'tpr_codigo_proyecto' : 'tie_codigo_proyecto','tpr_regional' : 'tie_regional','tpr_macroproyecto' : 'tie_macroproyecto', 'stg_etapa_proyecto' : 'tie_etapa', 'tpr_proyecto' : 'tie_proyecto', 'stg_fecha_corte' : 'tie_fecha_corte'})
    tbl_inicio_escrituracion['tie_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tbl_inicio_escrituracion['tie_lote_proceso']=current_bash

    tbl_inicio_escrituracion=tbl_inicio_escrituracion.reindex(columns=['tie_regional',
                                                                        'tie_codigo_proyecto',
                                                                        'tie_macroproyecto',
                                                                        'tie_proyecto',
                                                                        'tie_etapa',
                                                                        'tie_dias_atraso',
                                                                        'tie_inicio_escrituracion_proyectado',
                                                                        'tie_inicio_escrituracion_programado',
                                                                        'tie_poder_fiduciaria_proyectado',
                                                                        'tie_poder_fiduciaria_programado',
                                                                        'tie_salida_rph_proyectado',
                                                                        'tie_salida_rph_programado',
                                                                        'tie_cierre_rph_proyectado',
                                                                        'tie_cierre_rph_programado',
                                                                        'tie_licencia_ph_proyectado',
                                                                        'tie_licencia_ph_programado',
                                                                        'tie_modificacion_lc_proyectado',
                                                                        'tie_modificacion_lc_programado',
                                                                        'tie_radic_modif_lc_proyectado',
                                                                        'tie_radic_modif_lc_programado',
                                                                        'tie_fecha_corte',
                                                                        'tie_fecha_proceso',
                                                                        'tie_lote_proceso'])

    print("   -Inicio Escrituracion ending")

    return tbl_inicio_escrituracion