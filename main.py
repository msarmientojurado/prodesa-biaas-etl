
""" This ETL is implemented as a series of python scripts, which 
    takes the information stored into an .xlsx file and transform 
    accordingly, to finally persist as a BigQuery table set."""

__author__      = "Miguel Sarmiento"
__copyright__   = "Copyright 2021, ProCibernetica"

# External Libraries

from staging_area import staging
from libraries.settings import ORIGIN_FILE
import pandas as pd
import numpy as np

# Internal Libraries 
from libraries.mirror import mirror

def main():
    print("Starting ETL process...");
    
    # Executing Mirror Area
    esp_consolidado_corte=mirror(ORIGIN_FILE);
    print("Uploaded rows: {}".format(len(esp_consolidado_corte.index)));

    #Executing Staging Area
    stg_consolidado_corte = staging(esp_consolidado_corte);

    

if __name__ == "__main__":
    main()



