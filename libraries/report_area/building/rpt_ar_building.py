import pandas as pd

from libraries.report_area.building.rpt_ar_building_file import rpt_ar_building_file

def rpt_building(tmp_proyectos_construccion):
    cut_date = pd.to_datetime(tmp_proyectos_construccion.tpc_fecha_corte.unique()[0])
    regions = tmp_proyectos_construccion['tpc_regional'].unique()
    for region in regions:
        rpt_ar_building_file(tmp_proyectos_construccion[tmp_proyectos_construccion['tpc_regional']==region],region,cut_date)

    return