import pandas as pd

from libraries.report_area.milestones.rpt_ar_milestones_file import rpt_ar_milestones_file

def rpt_ar_milestones(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion):
    cut_date = pd.to_datetime(tbl_inicio_venta.tiv_fecha_corte.unique()[0])
    #regions = tmp_proyectos_construccion['tpc_regional'].unique()
    #for region in regions:
        #rpt_ar_milestones_file(tmp_proyectos_construccion[tmp_proyectos_construccion['tpc_regional']==region],region,cut_date)
    rpt_ar_milestones_file(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion,
            cut_date)
    return