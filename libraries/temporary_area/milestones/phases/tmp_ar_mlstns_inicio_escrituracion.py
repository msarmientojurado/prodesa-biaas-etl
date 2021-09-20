
import pandas as pd

def tmp_ar_mlstns_inicio_escrituracion(milestones_dataset,tbl_proyectos):

    print("   *Inicio Escrituracion Starting")

    tbl_inicio_escrituracion=pd.DataFrame()
    tbl_inicio_escrituracion=milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "1.INICIO ESCRITURACION"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_inicio_escrituracion_programado', 'stg_fecha_fin_planeada':'tie_inicio_escrituracion_proyectado'})
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "2.Poder firma de escrituras"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_poder_fiduciaria_programado', 'stg_fecha_fin_planeada':'tie_poder_fiduciaria_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "3.Salida de Registro RPH y Entrega de Folios"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_salida_rph_programado', 'stg_fecha_fin_planeada':'tie_salida_rph_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[(milestones_dataset['stg_nombre_actividad'] == "4.Cierre y numeracion de escritura de RPH") | (milestones_dataset['stg_nombre_actividad'] == "4.Cierre y numeración de escritura de RPH")].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_cierre_rph_programado', 'stg_fecha_fin_planeada':'tie_cierre_rph_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "5.Ejecutoria y entrega de licencia PH"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_licencia_ph_programado', 'stg_fecha_fin_planeada':'tie_licencia_ph_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "6.Modificación LC"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_modificacion_lc_programado', 'stg_fecha_fin_planeada':'tie_modificacion_lc_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion, milestones_dataset[milestones_dataset['stg_nombre_actividad'] == "7.Radicacion Modificación Licencia de Construcción"].loc[:,('key','stg_fecha_fin_planeada','stg_fecha_fin')].rename(columns={'stg_fecha_fin':'tie_radic_modif_lc_programado', 'stg_fecha_fin_planeada':'tie_radic_modif_lc_proyectado'}), on='key', how="outer",)
    tbl_inicio_escrituracion['tie_dias_atraso']=(tbl_inicio_escrituracion['tie_inicio_escrituracion_programado']-tbl_inicio_escrituracion['tie_inicio_escrituracion_proyectado']).dt.days
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion,milestones_dataset.loc[:, ('key','stg_codigo_proyecto','stg_etapa_proyecto')].groupby(by=["key"]).first().reset_index(), on='key', how="left",)
    tbl_inicio_escrituracion = tbl_inicio_escrituracion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
    tbl_inicio_escrituracion=pd.merge(tbl_inicio_escrituracion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
    tbl_inicio_escrituracion = tbl_inicio_escrituracion.rename(columns={'tpr_codigo_proyecto' : 'tie_codigo_proyecto','tpr_regional' : 'tie_regional','tpr_macroproyecto' : 'tie_macroproyecto', 'stg_etapa_proyecto' : 'tie_etapa', 'tpr_proyecto' : 'tie_proyecto'})

    print("   -Inicio Escrituracion ending")

    return tbl_inicio_escrituracion