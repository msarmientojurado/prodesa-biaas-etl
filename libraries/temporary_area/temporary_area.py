
#This section builds the final reports needed by Prodesa
from libraries.temporary_area.building.tmp_ar_building import tmp_ar_building
from libraries.temporary_area.planning.tmp_ar_planning import tmp_ar_planning
from libraries.temporary_area.commercial.tmp_ar_commercial import tmp_ar_commercial
from libraries.temporary_area.milestones.tmp_ar_milestones import tmp_ar_milestones
from libraries.temporary_area.parametrization.tmp_ar_parametrization import tmp_ar_parametrization

def temporary_area(stg_consolidado_corte):
    print(" *Temporary Area Starting")

    #Executing Parametrization Script
    tbl_proyectos=tmp_ar_parametrization();

    #Hitos
    tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion = tmp_ar_milestones(stg_consolidado_corte, tbl_proyectos)
    
    #Construccion
    tmp_proyectos_construccion = tmp_ar_building(stg_consolidado_corte, tbl_proyectos)

    #Consolidado Proyectos de Planeacion
    tmp_proyectos_planeacion = tmp_ar_planning(stg_consolidado_corte, tbl_proyectos)

    #TODO Implement "Consolidado Proyectos Comercial"
    tmp_proyectos_comercial = tmp_ar_commercial(stg_consolidado_corte)

    #TODO Implement "Reporte por Entregas"
    
    print(" -Temporary Area ending")
    return tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial

    
