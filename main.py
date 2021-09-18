
""" This ETL is implemented as a series of python scripts, which 
    takes the information stored into an .xlsx file and transform 
    accordingly, to finally persist as a BigQuery table set."""

__author__      = "Miguel Sarmiento"
__copyright__   = "Copyright 2021, ProCibernetica"

# External Libraries

from libraries.model import model
from libraries.temporary import temporary
from libraries.staging_area import staging
from libraries.settings import ORIGIN_FILE
import pandas as pd
import numpy as np

# Internal Libraries 
from libraries.mirror import mirror

def main():
    print("Starting ETL process...");
    
    # Running Mirror Area
    esp_consolidado_corte=mirror(ORIGIN_FILE);
    #print(" * Uploaded rows: {}".format(len(esp_consolidado_corte.index)));

    #Running Staging Area
    stg_consolidado_corte = staging(esp_consolidado_corte);

    #Running Temporary Area
    tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion, tmp_proyectos_comercial = temporary(stg_consolidado_corte);

    #TODO Implementation of Model Area
    model(tmp_proyectos_construccion)

    print("Ending ETL process...");

if __name__ == "__main__":
    main()



