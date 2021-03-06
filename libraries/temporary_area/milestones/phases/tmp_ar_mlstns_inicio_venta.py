
import pandas as pd
import numpy as np

def tmp_ar_mlstns_inicio_venta(milestones_dataset, tbl_proyectos, current_bash):

    print("   *Inicio Ventas Starting")

    #start_selling=milestones_dataset[milestones_dataset['stg_programacion_proyecto'] == "IV"]
    start_selling=milestones_dataset
    
    tbl_inicio_venta=pd.DataFrame()

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "1.FIN INICIO DE VENTAS"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_inicio_ventas_programado', 'total':'tiv_inicio_ventas_proyectado'})

    auxCol= start_selling[start_selling['stg_nombre_actividad'] == "2.Aprobacion FIV"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_fiv_programado', 'total':'tiv_fiv_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "3.Presupuesto Tipo FIV"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_ppto_revisado_programado', 'total':'tiv_ppto_revisado_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Entrega de Documentos para elaboraci??n del presupuesto IV"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_docs_ppto_programado', 'total':'tiv_docs_ppto_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "4.Encargo Fiduciario"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_encargo_fiduciario_programado', 'total':'tiv_encargo_fiduciario_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "5. Kit Entrega 1 a comercial"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_kit_comercial_programado', 'total':'tiv_kit_comercial_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "8.Validacion de Sala de Ventas y Modelos"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_val_sv_model_programado', 'total':'tiv_val_sv_model_proyectado'}), on='key', how="outer",)

    auxCol= start_selling[start_selling['stg_nombre_actividad'] == "9.Construcci??n de sala de ventas y modelos"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_const_sv_model_programado', 'total':'tiv_const_sv_model_proyectado'}), on='key', how="outer",)

    auxCol= start_selling[start_selling['stg_nombre_actividad'] == "10. Aprobaci??n LC"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_aprobacion_lc_programado', 'total':'tiv_aprobac_lc_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "11. Radicaci??n de LC"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_radicacion_lc_programado', 'total':'tiv_radicacion_lc_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Dise??o para salida a ventas"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_salida_ventas_programado', 'total':'tiv_salida_ventas_proyectado'}), on='key', how="outer",)

    auxCol= start_selling[start_selling['stg_nombre_actividad'] == "Acta de Constituci??n - (Kit 0 Comercial)"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_acta_constituc_programado', 'total':'tiv_acta_constituc_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "VoBo Comit?? de proyectos - Presentaci??n Esquema B??sico"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_visto_bueno_programado', 'total':'tiv_visto_bueno_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Elaboraci??n de alternativas"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_elab_alternativ_programado', 'total':'tiv_elab_alternativ_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Definici??n de producto objetivo"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_prod_objetivo_programado', 'total':'tiv_prod_objetivo_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Lluvia de ideas"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_lluvia_ideas_programado', 'total':'tiv_lluvia_ideas_proyectado'}), on='key', how="outer",)

    auxCol=start_selling[start_selling['stg_nombre_actividad'] == "Kit Desarrollos"].loc[:,('key','stg_fecha_final_actual','stg_fin_linea_base_estimado', 'stg_fecha_fin_planeada')]
    auxCol['total']=np.where(auxCol['stg_fecha_final_actual'].isna(),auxCol['stg_fecha_fin_planeada'],auxCol['stg_fecha_final_actual'])
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, auxCol.loc[:,('key','stg_fin_linea_base_estimado', 'total')].rename(columns={'stg_fin_linea_base_estimado':'tiv_kit_desarrollos_programado', 'total':'tiv_kit_desarrollos_proyectado'}), on='key', how="outer",)

    tbl_inicio_venta['tiv_dias_atraso']=(tbl_inicio_venta['tiv_inicio_ventas_programado']-tbl_inicio_venta['tiv_inicio_ventas_proyectado']).dt.days
    tbl_inicio_venta=pd.merge(tbl_inicio_venta,start_selling.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_venta = tbl_inicio_venta.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_venta=pd.merge(tbl_inicio_venta,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_venta['stg_fecha_corte'] = start_selling['stg_fecha_corte'].iloc[0]
    tbl_inicio_venta = tbl_inicio_venta.rename(columns={'tpr_codigo_proyecto' : 'tiv_codigo_proyecto','tpr_regional' : 'tiv_regional','tpr_macroproyecto' : 'tiv_macroproyecto', 'stg_etapa_proyecto' : 'tiv_etapa', 'tpr_proyecto' : 'tiv_proyecto', 'stg_fecha_corte' : 'tiv_fecha_corte'})
    tbl_inicio_venta['tiv_fecha_proceso']=pd.to_datetime(pd.to_datetime("today").strftime("%m/%d/%Y"))
    tbl_inicio_venta['tiv_lote_proceso']=current_bash

    
    tbl_inicio_venta=tbl_inicio_venta.reindex(columns=['tiv_regional',
                                                    'tiv_codigo_proyecto',
                                                    'tiv_macroproyecto',
                                                    'tiv_proyecto',
                                                    'tiv_etapa',
                                                    'tiv_dias_atraso',
                                                    'tiv_inicio_ventas_proyectado',
                                                    'tiv_inicio_ventas_programado',
                                                    'tiv_fiv_proyectado',
                                                    'tiv_fiv_programado',
                                                    'tiv_ppto_revisado_proyectado',
                                                    'tiv_ppto_revisado_programado',
                                                    'tiv_docs_ppto_proyectado',
                                                    'tiv_docs_ppto_programado',
                                                    'tiv_encargo_fiduciario_proyectado',
                                                    'tiv_encargo_fiduciario_programado',
                                                    'tiv_kit_comercial_proyectado',
                                                    'tiv_kit_comercial_programado',
                                                    'tiv_val_sv_model_proyectado',
                                                    'tiv_val_sv_model_programado',
                                                    'tiv_const_sv_model_proyectado',
                                                    'tiv_const_sv_model_programado',
                                                    'tiv_aprobac_lc_proyectado',
                                                    'tiv_aprobacion_lc_programado',
                                                    'tiv_radicacion_lc_proyectado',
                                                    'tiv_radicacion_lc_programado',
                                                    'tiv_salida_ventas_proyectado',
                                                    'tiv_salida_ventas_programado',
                                                    'tiv_acta_constituc_proyectado',
                                                    'tiv_acta_constituc_programado',
                                                    'tiv_visto_bueno_proyectado',
                                                    'tiv_visto_bueno_programado',
                                                    'tiv_elab_alternativ_proyectado',
                                                    'tiv_elab_alternativ_programado',
                                                    'tiv_prod_objetivo_proyectado',
                                                    'tiv_prod_objetivo_programado',
                                                    'tiv_lluvia_ideas_proyectado',
                                                    'tiv_lluvia_ideas_programado',
                                                    'tiv_kit_desarrollos_proyectado',
                                                    'tiv_kit_desarrollos_programado',
                                                    'tiv_fecha_corte',
                                                    'tiv_fecha_proceso',
                                                    'tiv_lote_proceso'])
    
    
    print("   -Inicio Ventas Ending")

    return tbl_inicio_venta
