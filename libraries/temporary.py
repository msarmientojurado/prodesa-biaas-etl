
#This section build the final reports needed by Prodesa

from libraries.commercial import commercial
from libraries.parametrization import parametrization
from libraries.planning import planning
from libraries.building import building
from libraries.milestones import milestones


def temporary(stg_consolidado_corte):

    print(" *Temporary Area Starting")

    #Executing Parametrization Script
    tbl_proyectos=parametrization();

    #Hitos
    tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion = milestones(stg_consolidado_corte, tbl_proyectos)
    
    #Construccion
    tmp_proyectos_construccion = building(stg_consolidado_corte, tbl_proyectos)

    #Consolidado Proyectos de Planeacion
    tmp_proyectos_planeacion = planning(stg_consolidado_corte, tbl_proyectos)

    #TODO Implement "Consolidado Proyectos Comercial"
    tmp_proyectos_comercial = commercial(stg_consolidado_corte)

    #TODO Implement "Reporte por Entregas"
    
    print(" -Temporary Area ending")
    
    return tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial

    
