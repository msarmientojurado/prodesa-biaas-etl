
import pandas as pd

def tmp_ar_mlstns_inicio_venta(milestones_dataset, tbl_proyectos):

    print("   *Inicio Ventas Starting")

    start_selling=milestones_dataset[milestones_dataset['stg_programacion_proyecto'] == "IV"]
    tbl_inicio_venta=pd.DataFrame()
    tbl_inicio_venta=start_selling[start_selling['stg_nombre_actividad'] == "1.FIN INICIO DE VENTAS"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_inicio_ventas_programado', 'stg_fecha_fin_planeada':'tiv_inicio_ventas_proyectado'})
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "2.Aprobacion FIV"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_fiv_programado', 'stg_fecha_fin_planeada':'tiv_fiv_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "3.Presupuesto Tipo FIV"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_ppto_revisado_programado', 'stg_fecha_fin_planeada':'tiv_ppto_revisado_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Entrega de Documentos para elaboración del presupuesto IV"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_docs_ppto_programado', 'stg_fecha_fin_planeada':'tiv_docs_ppto_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "4.Encargo Fiduciario"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_encargo_fiduc_programado', 'stg_fecha_fin_planeada':'tiv_encargo_fiduc_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "5. Kit Entrega 1 a comercial"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_kit_comercial_programado', 'stg_fecha_fin_planeada':'tiv_kit_comercial_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "8.Validacion de Sala de Ventas y Modelos"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_val_sv_model_programado', 'stg_fecha_fin_planeada':'tiv_val_sv_model_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "9.Construcción de sala de ventas y modelos"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_const_sv_model_programado', 'stg_fecha_fin_planeada':'tiv_const_sv_model_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "10. Aprobación LC"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_aprobacion_lc_programado', 'stg_fecha_fin_planeada':'tiv_aprobac_lc_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "11. Radicación de LC"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_radicacion_lc_programado', 'stg_fecha_fin_planeada':'tiv_radicacion_lc_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Diseño para salida a ventas"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_salida_ventas_programado', 'stg_fecha_fin_planeada':'tiv_salida_ventas_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Acta de Constitución - (Kit 0 Comercial)"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_acta_constituc_programado', 'stg_fecha_fin_planeada':'tiv_acta_constituc_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "VoBo Comité de proyectos - Presentación Esquema Básico"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_visto_bueno_programado', 'stg_fecha_fin_planeada':'tiv_visto_bueno_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Kit Desarrollos"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_elab_alternativ_programado', 'stg_fecha_fin_planeada':'tiv_elab_alternativ_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Definición de producto objetivo"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_prod_objetivo_programado', 'stg_fecha_fin_planeada':'tiv_prod_objetivo_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta=pd.merge(tbl_inicio_venta, start_selling[start_selling['stg_nombre_actividad'] == "Lluvia de ideas"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tiv_lluvia_ideas_programado', 'stg_fecha_fin_planeada':'tiv_lluvia_ideas_proyectado'}), on='key', how="outer",)
    tbl_inicio_venta['tiv_dias_atraso']=(tbl_inicio_venta['tiv_inicio_ventas_programado']-tbl_inicio_venta['tiv_inicio_ventas_proyectado']).dt.days
    tbl_inicio_venta=pd.merge(tbl_inicio_venta,start_selling.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_venta = tbl_inicio_venta.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_venta=pd.merge(tbl_inicio_venta,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_venta['stg_fecha_corte'] = start_selling['stg_fecha_corte'].iloc[0]
    tbl_inicio_venta = tbl_inicio_venta.rename(columns={'tpr_codigo_proyecto' : 'tiv_codigo_proyecto','tpr_regional' : 'tiv_regional','tpr_macroproyecto' : 'tiv_macroproyecto', 'stg_etapa_proyecto' : 'tiv_etapa', 'tpr_proyecto' : 'tiv_proyecto', 'stg_fecha_corte' : 'tiv_fecha_corte'})
    tbl_inicio_venta['tiv_fecha_proceso']=pd.to_datetime("today")
    tbl_inicio_venta['tiv_lote_proceso']=1

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
                                                    'tiv_encargo_fiduc_proyectado',
                                                    'tiv_encargo_fiduc_programado',
                                                    'tiv_kit_comercial_proyectado',
                                                    'tiv_kit_comercial_programado',
                                                    'tiv_val_sv_model_proyectado',
                                                    'tiv_val_sv_model_programado',
                                                    'tiv_const_sv_model_proyectado',
                                                    'tiv_const_sv_model_programado',
                                                    'tiv_aprobac_lc_proyectado',
                                                    'tiv_aprobacion_lc_programado',
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
                                                    'tiv_fecha_corte',
                                                    'tiv_fecha_proceso',
                                                    'tiv_lote_proceso'])
    
    print("   -Inicio Ventas Ending")

    return tbl_inicio_venta
