import pandas as pd

from libraries.report_area.planning.rpt_ar_planning_file import rpt_ar_planning_file



def rpt_planning(tmp_proyectos_planeacion):
    cut_date = pd.to_datetime(tmp_proyectos_planeacion.tpp_fecha_corte.unique()[0])
    regions = tmp_proyectos_planeacion['tpp_regional'].unique()
    for region in regions:
        rpt_ar_planning_file(tmp_proyectos_planeacion[tmp_proyectos_planeacion['tpp_regional']==region],region,cut_date)

    return