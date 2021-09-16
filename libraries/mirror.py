
import pandas as pd

def mirror(file_name):
    print("Mirror Area Starting...");
    
    #Uploading Consolidado
    esp_consolidado_corte = pd.read_excel(file_name);

    print("Mirror Area ending...");

    return esp_consolidado_corte;
    