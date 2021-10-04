
from libraries.model_area.building.mdl_ar_building import mdl_ar_building
from libraries.model_area.commercial.mdl_ar_commercial import mdl_ar_commercial
from libraries.model_area.deliveries.mdl_ar_deliveries import mdl_ar_deliveries
from libraries.model_area.planning.mdl_ar_planning import mdl_ar_planning
from libraries.model_area.milestones.mdl_ar_milestones import model_milestones
from libraries.settings import ENVIRONMENT


def model(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion, 
            tmp_proyectos_construccion, 
            tmp_proyectos_planeacion,
            tmp_proyectos_comercial,
            tbl_reporte_por_entregas, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution
        ):

    print(" *Model Starting...")

    if ENVIRONMENT == "Production":
        
        #"Consolidado de Proyectos de Construccion"
        if building_report_excecution ==True:
            mdl_ar_building(tmp_proyectos_construccion)
        
        #"Reporte por entrega"
        if building_report_excecution ==True:
            mdl_ar_deliveries(tbl_reporte_por_entregas)

        #"Consolidado de Proyectos de Planeacion"
        if planning_report_excecution ==True:
            mdl_ar_planning(tmp_proyectos_planeacion)

        #"Consolidado de Proyectos de Comercial"
        if commercial_report_excecution ==True:
            mdl_ar_commercial(tmp_proyectos_comercial)

        #"Control de Hitos de Planeacion"
        model_milestones(tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion)

        
    print(" *Model ending...")