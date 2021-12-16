import pandas as pd
from libraries.report_area.deliveries.rpt_ar_deliveries_excel_file import rpt_ar_deliveries_excel_file

from libraries.report_area.deliveries.rpt_ar_deliveries_file import rpt_ar_deliveries_file

def rpt_ar_deliveries(tbl_reporte_por_entregas):

    print("  *Deliveries Starting")

    #tbl_reporte_por_entregas_filter = pd.DataFrame()
    tbl_reporte_por_entregas_filter = tbl_reporte_por_entregas.copy(deep=True)
    tbl_reporte_por_entregas_filter['key']=tbl_reporte_por_entregas_filter['trpe_proyecto']+'_'+tbl_reporte_por_entregas_filter['trpe_etapa']
    
    #reports=tbl_reporte_por_entregas_filter['key'].unique()
    
    #rpt_ar_deliveries_file(tbl_reporte_por_entregas_filter[tbl_reporte_por_entregas_filter['key']==reports[0]])
    cut_date = pd.to_datetime(tbl_reporte_por_entregas.trpe_fecha_corte.unique()[0])

    regions = tbl_reporte_por_entregas_filter['trpe_regional'].unique()
    excel_report_array=[]
    for region in regions:
        tmp = rpt_ar_deliveries_excel_file(tbl_reporte_por_entregas_filter.loc[tbl_reporte_por_entregas_filter['trpe_regional']==region], cut_date)
        excel_report_array.append([tmp, "Entregas","corte_" +  cut_date.strftime('%d-%m-%Y') + "_entregas_" + region + ".xlsx", cut_date])
    
    print("  -Deliveries Starting")

    return excel_report_array