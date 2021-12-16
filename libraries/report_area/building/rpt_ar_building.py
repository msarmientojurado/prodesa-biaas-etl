import pandas as pd

from libraries.report_area.building.rpt_ar_building_file import rpt_ar_building_file

def rpt_building(tmp_proyectos_construccion):
    
    print("  *Building Starting")

    cut_date = pd.to_datetime(tmp_proyectos_construccion.tpc_fecha_corte.unique()[0])
    regions = tmp_proyectos_construccion['tpc_regional'].unique()
    excel_report_array=[]
    for region in regions:
        tmp = rpt_ar_building_file(tmp_proyectos_construccion[tmp_proyectos_construccion['tpc_regional']==region],region,cut_date)
        excel_report_array.append([tmp, "Construccion","corte_" +  cut_date.strftime('%d-%m-%Y') + "_construccion_" + region + ".xlsx", cut_date])
    
    print("  -Building Starting")

    return excel_report_array