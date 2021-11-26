import pandas as pd

from libraries.report_area.deliveries.rpt_ar_deliveries_file import rpt_ar_deliveries_file

def rpt_ar_deliveries(tbl_reporte_por_entregas):

    #tbl_reporte_por_entregas_filter = pd.DataFrame()
    tbl_reporte_por_entregas_filter = tbl_reporte_por_entregas.copy(deep=True)
    tbl_reporte_por_entregas_filter['key']=tbl_reporte_por_entregas_filter['trpe_proyecto']+'_'+tbl_reporte_por_entregas_filter['trpe_etapa']
    
    reports=tbl_reporte_por_entregas_filter['key'].unique()
    
    rpt_ar_deliveries_file(tbl_reporte_por_entregas_filter[tbl_reporte_por_entregas_filter['key']==reports[0]])

    return