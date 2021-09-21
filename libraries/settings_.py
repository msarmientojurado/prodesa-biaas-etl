ENVIRONMENT = "Development"
# This variable is used to switch between production and development environments.
#   Values:
#       Production
#       Development

#File name when application is in development environment
ORIGIN_FILE = "Consolidado_Excel.xlsx"

#Bucket Name when application is in production (GCP) environment
BUCKET_NAME = "prodesa-biaas-bucket"
#File name when application is in Production (GCP) environment
BLOB_NAME = "Consolidado_Excel_13-08-2021.xlsx"

#BigQuery parammeters
TBL_PROYECTOS_CONSTRUCCION = 'modelo_biaas_python_test.tbl_proyectos_construccion_test3'
TBL_PROYECTOS_PLANEACION = 'modelo_biaas_python_test.tbl_proyectos_planeacion_test3'
TBL_INICIO_VENTA = 'modelo_biaas_python_test.tbl_inicio_venta_test3'
TBL_INICIO_PROMESA = 'modelo_biaas_python_test.tbl_inicio_promesa_test3'
TBL_INICIO_CONSTRUCCION = 'modelo_biaas_python_test.tbl_inicio_construccion_test3'
TBL_INICIO_ESCRITURACION = 'modelo_biaas_python_test.tbl_inicio_escrituracion_test3'