
from libraries.model_area.backup_builder.backup_builder import backup_builder
from libraries.model_area.building.mdl_ar_building import mdl_ar_building
from libraries.model_area.commercial.mdl_ar_commercial import mdl_ar_commercial
from libraries.model_area.deliveries.mdl_ar_deliveries import mdl_ar_deliveries
from libraries.model_area.download_reports.download_reports import download_reports
from libraries.model_area.graphics.mdl_ar_graphics import mdl_ar_graphics
from libraries.model_area.loading_control.loading_control import loading_control
from libraries.model_area.mdl_ar_cleaning_db import mdl_ar_cleaning_db
from libraries.model_area.planning.mdl_ar_planning import mdl_ar_planning
from libraries.model_area.milestones.mdl_ar_milestones import model_milestones
from libraries.various.store_process_result import store_process_result
import pandas as pd


def model(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion, 
            tmp_proyectos_construccion, 
            tmp_proyectos_planeacion,
            tmp_proyectos_comercial,
            tbl_reporte_por_entregas,
            tbl_graficos_tiempo_avance_buffer, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution,
            output_file_content,
            source_file_name,
            data_bytes,
            bash,
            stg_consolidado_corte,
            report_url
        ):

    print(" *Model Starting...")

    #Cleaning Tables Before persisiting new data
    cut_date = pd.to_datetime(tbl_graficos_tiempo_avance_buffer.tgabt_fecha_corte.unique()[0])
    mdl_ar_cleaning_db(cut_date.strftime("%Y-%m-%d"))
    
    #"Grafico Tiempo VS Avance & Avance VS Consumo Buffer"
    mdl_ar_graphics(tbl_graficos_tiempo_avance_buffer)

    #"Consolidado de Proyectos de Construccion"
    if building_report_excecution ==True:
        mdl_ar_building(tmp_proyectos_construccion)
    
    #"Reporte por entrega"
    if building_report_excecution ==True:
        mdl_ar_deliveries(tbl_reporte_por_entregas)

    #"Consolidado de Proyectos de Planeacion"
    if planning_report_excecution ==True:
        mdl_ar_planning(tmp_proyectos_planeacion)

    #"Consolidado de Proyectos de Comercial"
    if commercial_report_excecution ==True:
        mdl_ar_commercial(tmp_proyectos_comercial)

    #"Control de Hitos de Planeacion"
    model_milestones(tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion)

    #Build Backup
    backup_filename = backup_builder(data_bytes, pd.to_datetime((stg_consolidado_corte.stg_fecha_corte.unique())[0]),bash)


    #Loading Control
    loading_control(source_file_name, backup_filename, bash, len(stg_consolidado_corte))

    #Loading Download Excel Report link
    download_reports(report_url, cut_date, bash)

    #Complete and Close the ETL result process

    file_result_content = "\n\n\t\t-- RESUMEN DEL PROCESO --\n\nArchivo Fuente: "+source_file_name+"\nArchivo Historico:"+backup_filename+"\nResultado Final del Proceso: Conforme\nFecha de Corte:" + (pd.to_datetime((stg_consolidado_corte.stg_fecha_corte.unique())[0])).strftime('%d-%m-%Y')
    output_file_content.write(file_result_content)

    store_process_result(output_file_content)
        
    print(" *Model ending...")