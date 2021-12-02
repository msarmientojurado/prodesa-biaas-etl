
#This section builds the final reports needed by Prodesa
from libraries.temporary_area.building.tmp_ar_building import tmp_ar_building
from libraries.temporary_area.deliveries.tmp_ar_deliveries import tmp_ar_deliveries
from libraries.temporary_area.graphics.tmp_ar_graphics import tmp_ar_graphics
from libraries.temporary_area.planning.tmp_ar_planning import tmp_ar_planning
from libraries.temporary_area.commercial.tmp_ar_commercial import tmp_ar_commercial
from libraries.temporary_area.milestones.tmp_ar_milestones import tmp_ar_milestones
from libraries.temporary_area.parametrization.tmp_ar_parametrization import tmp_ar_parametrization

import pandas as pd

def temporary_area(stg_consolidado_corte, current_bash):
    print(" *Temporary Area Starting")

    #Executing Parametrization Script
    tbl_proyectos, building_report_excecution, planning_report_excecution, commercial_report_excecution=tmp_ar_parametrization(stg_consolidado_corte);
    
    #Hitos
    tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion = tmp_ar_milestones(stg_consolidado_corte, tbl_proyectos, current_bash)
    
    #Construccion
    if building_report_excecution == True:
        tmp_proyectos_construccion = tmp_ar_building(stg_consolidado_corte, tbl_proyectos, current_bash)
        tbl_reporte_por_entregas = tmp_ar_deliveries(stg_consolidado_corte, tbl_proyectos, current_bash)
    else:
        tmp_proyectos_construccion= pd.DataFrame()
        tbl_reporte_por_entregas= pd.DataFrame()
    

    #Consolidado Proyectos de Planeacion
    if planning_report_excecution == True:
        tmp_proyectos_planeacion = tmp_ar_planning(stg_consolidado_corte, tbl_proyectos, current_bash)
    else:
        tmp_proyectos_planeacion= pd.DataFrame()

    #Consolidado Proyectos Comercial
    if commercial_report_excecution == True:
        tmp_proyectos_comercial = tmp_ar_commercial(stg_consolidado_corte, tbl_proyectos, current_bash)
    else:
        tmp_proyectos_comercial= pd.DataFrame()

    #Graficos
    tbl_graficos_tiempo_avance_buffer = tmp_ar_graphics(stg_consolidado_corte, 
                                                        current_bash,
                                                        tmp_proyectos_construccion, 
                                                        tmp_proyectos_planeacion, 
                                                        tmp_proyectos_comercial,
                                                        building_report_excecution, 
                                                        planning_report_excecution,
                                                        commercial_report_excecution
                                                        )
    
    print(" -Temporary Area ending")
    return tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial, tbl_reporte_por_entregas, tbl_graficos_tiempo_avance_buffer, building_report_excecution, planning_report_excecution, commercial_report_excecution
