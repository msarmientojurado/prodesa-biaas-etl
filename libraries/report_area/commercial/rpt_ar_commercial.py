import pandas as pd

from libraries.report_area.building.rpt_ar_building_file import rpt_ar_building_file
from libraries.report_area.commercial.rpt_ar_commercial_file import rpt_ar_commercial_file

def rpt_ar_commercial(tbl_proyectos_comercial):
    cut_date = pd.to_datetime(tbl_proyectos_comercial.tpcm_fecha_corte.unique()[0])
    regions = tbl_proyectos_comercial['tpcm_regional'].unique()
    for region in regions:
        rpt_ar_commercial_file(tbl_proyectos_comercial[tbl_proyectos_comercial['tpcm_regional']==region],region,cut_date)

    return