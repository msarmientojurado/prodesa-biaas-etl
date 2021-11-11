
from libraries.report_area.building.rpt_ar_building import rpt_building
from libraries.report_area.planning.rpt_ar_planning import rpt_planning


def report_area(tmp_proyectos_construccion, 
            tmp_proyectos_planeacion, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution):
    if building_report_excecution==True:
        rpt_building(tmp_proyectos_construccion)
    if planning_report_excecution == True:
        rpt_planning(tmp_proyectos_planeacion)
    return