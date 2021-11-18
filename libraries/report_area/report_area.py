
from libraries.report_area.building.rpt_ar_building import rpt_building
from libraries.report_area.commercial.rpt_ar_commercial import rpt_ar_commercial
from libraries.report_area.milestones.rpt_ar_milestones import rpt_ar_milestones
from libraries.report_area.planning.rpt_ar_planning import rpt_planning


def report_area(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion, 
            tmp_proyectos_construccion, 
            tmp_proyectos_planeacion,
            tmp_proyectos_comercial, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution):
    if building_report_excecution==True:
        rpt_building(tmp_proyectos_construccion)
    if planning_report_excecution == True:
        rpt_planning(tmp_proyectos_planeacion)
        rpt_ar_milestones(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion)
    if commercial_report_excecution==True:
        rpt_ar_commercial(tmp_proyectos_comercial)
    return