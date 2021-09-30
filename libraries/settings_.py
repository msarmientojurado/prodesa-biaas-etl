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
BIGQUERY_ENVIRONMENT_NAME="proyecto-prodesa"
# This variable is used to switch between production and development environments.
#   Values:
#       proyecto-prodesa
#       production-prodesa-biaas

BIGQUERY_DATASET='modelo_biaas_python_test'

TBL_PROYECTOS_CONSTRUCCION = BIGQUERY_DATASET + '.tbl_proyectos_construccion'
TBL_PROYECTOS_PLANEACION = BIGQUERY_DATASET + '.tbl_proyectos_planeacion'
TBL_INICIO_VENTA = BIGQUERY_DATASET + '.tbl_inicio_venta'
TBL_INICIO_PROMESA = BIGQUERY_DATASET + '.tbl_inicio_promesa'
TBL_INICIO_CONSTRUCCION = BIGQUERY_DATASET + '.tbl_inicio_construccion'
TBL_INICIO_ESCRITURACION = BIGQUERY_DATASET + '.tbl_inicio_escrituracion'