
""" This ETL is implemented as a series of python scripts, which 
    takes the information stored into an .xlsx file and transform 
    accordingly, to finally persist as a BigQuery table set."""

__author__      = "Miguel Sarmiento"
__copyright__   = "Copyright 2021, ProCibernetica"



# Internal Libraries 
from libraries.mirror_area.mirror_area import mirror_area
from libraries.model_area.parametrization.parametrization import parametrization
from libraries.pre_process_area.pre_process_area import pre_process_area
from libraries.report_area.report_area import report_area
from libraries.staging_area.staging_area import staging_area
from libraries.temporary_area.temporary_area import temporary_area
from libraries.model_area.model_area import model

# External Libraries

import pandas as pd
import numpy as np

from libraries.validation_mesh.validation_mesh import validation_mesh

def main():
    print("Starting ETL process...");
    
    stop_process, report_file_content, source_file_name = pre_process_area()

    if stop_process == False:
        # Running Mirror Area
        esp_consolidado_corte, report_file_content, stop_process, data_bytes=mirror_area(source_file_name, report_file_content);
        #print(" * Uploaded rows: {}".format(len(esp_consolidado_corte.index)));
    else:
        print("Proceso no pudo ser terminado por resultado proceso de Pre-Procesamiento")
    
    if stop_process == False:
        esp_consolidado_corte, stop_process, report_file_content= validation_mesh(esp_consolidado_corte,report_file_content)
    else:
        print("Proceso no pudo ser terminado porque el archivo no pudo ser cargado")

    if stop_process == False:
        #Running Staging Area
        current_bash = parametrization()

        stg_consolidado_corte = staging_area(esp_consolidado_corte);
    
        #Running Temporary Area
        tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial, tbl_reporte_por_entregas, tbl_graficos_tiempo_avance_buffer, building_report_excecution, planning_report_excecution, commercial_report_excecution = temporary_area(stg_consolidado_corte, current_bash);
        
        #print(stg_consolidado_corte.columns)
        
        #Report Area
        report_area(tmp_proyectos_construccion, 
            tmp_proyectos_planeacion, 
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution)

        #Model Area
        model(tbl_inicio_venta, 
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
            report_file_content,
            source_file_name,
            data_bytes,
            current_bash,
            stg_consolidado_corte
        )
        
    else:
        print("Proceso no pudo ser terminado por resultado de la malla de Validacion")


    print("Ending ETL process...");

if __name__ == "__main__":
    main()



