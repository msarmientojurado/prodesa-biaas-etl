import pandas as pd

from libraries.report_area.milestones.rpt_ar_milestones_file import rpt_ar_milestones_file

from zipfile import ZipFile
from tempfile import NamedTemporaryFile

from libraries.settings import BUCKET_NAME_DOWNLOAD_REPORT

from google.cloud import storage

def rpt_ar_milestones(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion):
    cut_date = pd.to_datetime(tbl_inicio_venta.tiv_fecha_corte.unique()[0])
    regions = tbl_inicio_venta['tiv_regional'].unique()
    excel_report_array=[]
    for region in regions:
        #rpt_ar_milestones_file(tmp_proyectos_construccion[tmp_proyectos_construccion['tpc_regional']==region],region,cut_date)
        tmp = rpt_ar_milestones_file(tbl_inicio_venta[tbl_inicio_venta['tiv_regional']==region], 
                tbl_inicio_promesa[tbl_inicio_promesa['tip_regional']==region], 
                tbl_inicio_construccion[tbl_inicio_construccion['tic_regional']==region], 
                tbl_inicio_escrituracion[tbl_inicio_escrituracion['tie_regional']==region],
                cut_date,
                region)
        excel_report_array.append([tmp, "Hitos","corte_" +  cut_date.strftime('%d-%m-%Y') + "_hitos_" + region + ".xlsx", cut_date])

    return excel_report_array