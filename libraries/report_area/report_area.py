
from libraries.report_area.building.rpt_ar_building import rpt_building
from libraries.report_area.commercial.rpt_ar_commercial import rpt_ar_commercial
from libraries.report_area.deliveries.rpt_ar_deliveries import rpt_ar_deliveries
from libraries.report_area.milestones.rpt_ar_milestones import rpt_ar_milestones
from libraries.report_area.planning.rpt_ar_planning import rpt_planning
from libraries.report_area.rpt_file_creation import rpt_file_creation


def report_area(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion, 
            tmp_proyectos_construccion, 
            tmp_proyectos_planeacion,
            tmp_proyectos_comercial,
            tbl_reporte_por_entregas, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution):
    excel_report_array=[]
    if building_report_excecution==True:
        excel_report_array +=rpt_building(tmp_proyectos_construccion)
        rpt_ar_deliveries(tbl_reporte_por_entregas)
    if planning_report_excecution == True:
        excel_report_array += rpt_planning(tmp_proyectos_planeacion)
        excel_report_array += rpt_ar_milestones(tbl_inicio_venta, 
                                            tbl_inicio_promesa, 
                                            tbl_inicio_construccion, 
                                            tbl_inicio_escrituracion)
    if commercial_report_excecution==True:
        excel_report_array +=rpt_ar_commercial(tmp_proyectos_comercial)
    
    report_url = rpt_file_creation(excel_report_array)

    return report_url