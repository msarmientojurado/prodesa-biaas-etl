
""" This ETL is implemented as a series of python scripts, which 
    takes the information stored into an .xlsx file and transform 
    accordingly, to finally persist as a BigQuery table set."""

__author__      = "Miguel Sarmiento"
__copyright__   = "Copyright 2021, ProCibernetica"



# Internal Libraries 
from libraries.mirror_area.mirror_area import mirror_area
from libraries.staging_area.staging_area import staging_area
from libraries.temporary_area.temporary_area import temporary_area
from libraries.model_area.model_area import model

# External Libraries

import pandas as pd
import numpy as np

def main():
    print("Starting ETL process...");
    
    # Running Mirror Area
    esp_consolidado_corte=mirror_area();
    #print(" * Uploaded rows: {}".format(len(esp_consolidado_corte.index)));

    #Running Staging Area
    stg_consolidado_corte, continue_process = staging_area(esp_consolidado_corte);

    if continue_process == True:
        #Running Temporary Area
        tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial, tbl_reporte_por_entregas, building_report_excecution, planning_report_excecution, commercial_report_excecution = temporary_area(stg_consolidado_corte);

        #TODO Implementation of Model Area
        model(tbl_inicio_venta, 
            tbl_inicio_promesa, 
            tbl_inicio_construccion, 
            tbl_inicio_escrituracion, 
            tmp_proyectos_construccion, 
            tmp_proyectos_planeacion,
            tmp_proyectos_comercial, 
            tbl_reporte_por_entregas,
            building_report_excecution, 
            planning_report_excecution,
            commercial_report_excecution
        )
    else:
        print("Proceso no pudo ser terminado por resultado de la malla de Validacion")

    print("Ending ETL process...");

if __name__ == "__main__":
    main()



