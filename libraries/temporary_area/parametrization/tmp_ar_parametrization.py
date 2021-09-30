

import pandas as pd
from google.cloud import bigquery
from libraries.settings import TBL_PROYECTOS, BIGQUERY_ENVIRONMENT_NAME, ENVIRONMENT

def tmp_ar_parametrization(stg_consolidado_corte):
    print("  *Parametrization Script Starting...");

    if ENVIRONMENT=="Development":
        data =['CALI','PINTURAS','PINTURAS','PINTURAS',True,'2021-08-27'],['CALI','SANTABARBARA','SANTABARBARA','SANTABARBARA',True,'2021-08-27'],['CALI','PASCUAL','PASCUAL','PASCUAL',True,'2021-08-27'],['BOGOTA','MADNV','ALTOS DE MADELENA','MADELENA',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-NOVIS','AMERICAN PIPE','AMERICAN PIPE NOVIS',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-VIP','AMERICAN PIPE','AMERICAN PIPE VIP',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-VIS','AMERICAN PIPE','AMERICAN PIPE VIS',True,'2021-08-27'],['BOGOTA','CALLE13','CALLE13','CALLE13',True,'2021-08-27'],['BOGOTA','CHANCO','CHANCO','CHANCO',True,'2021-08-27'],['BOGOTA','URDECO','CIPRES DE LA FLORIDA','CIPRES DE LA FLORIDA',True,'2021-08-27'],['BOGOTA','CVM55TV1','CIUDAD VERDE','YERBABUENA',True,'2021-08-27'],['BOGOTA','SMART2','EQUILIBRIUM','EQUILIBRIUM',True,'2021-08-27'],['BOGOTA','BELLAFLORA','BELLAFLORA','BELLAFLORA',True,'2021-08-27'],['BOGOTA','SNJORLCER','HACIENDA ALCALç','CEREZO',True,'2021-08-27'],['BOGOTA','SNJORROB','HACIENDA ALCALç','ROBLE',True,'2021-08-27'],['BOGOTA','SNJORL','HACIENDA ALCALç','SAUCE',True,'2021-08-27'],['BOGOTA','SNJORLT1','HACIENDA ALCALç','SAUCE TO1',True,'2021-08-27'],['BOGOTA','SNJORLT4','HACIENDA ALCALç','SAUCE TO4',True,'2021-08-27'],['BOGOTA','SNJORLT9','HACIENDA ALCALç','SAUCE TO9',True,'2021-08-27'],['BOGOTA','SNJORLSAM','HACIENDA ALCALç','SAMAN',True,'2021-08-27'],['BOGOTA','PDAW2TW','PALO DE AGUA','KATIOS',True,'2021-08-27'],['BOGOTA','PDAAISB','PALO DE AGUA','AISLADAS B',True,'2021-08-27'],['BOGOTA','PDAPAW','PALO DE AGUA','MACARENA',True,'2021-08-27'],['BOGOTA','PDASWTW','PALO DE AGUA','PALO DE AGUA',True,'2021-08-27'],['BOGOTA','PRAFU-MZ10','CIUDADELA FORESTA','MILANO',True,'2021-08-27'],['BOGOTA','PRAFU-MZ7','CIUDADELA FORESTA','IBIZ',True,'2021-08-27'],['BOGOTA','PRAFU-MZ2','CIUDADELA FORESTA','AMAZILIA',True,'2021-08-27'],['BOGOTA','PRAFU-MZ8','CIUDADELA FORESTA','ANDARRIOS',True,'2021-08-27'],['BOGOTA','PRAFU-MZ4','CIUDADELA FORESTA','CDF MZ4',True,'2021-08-27'],['BOGOTA','PRAFU-MZ5','CIUDADELA FORESTA','CDF MZ5',True,'2021-08-27'],['BOGOTA','RECREO','RECREO','RECREO',True,'2021-08-27'],['BOGOTA','SOLEM5','RESERVA DE MADRID','PALERMO',True,'2021-08-27'],['BOGOTA','SOLEM8','RESERVA DE MADRID','PAMPLONA',True,'2021-08-27'],['BOGOTA','SNHILARIO','SAN HILARIO','SAN HILARIO',True,'2021-08-27'],['BOGOTA','SNLUIS','SAN LUIS','SAN LUIS',True,'2021-08-27'],['BOGOTA','VINCULO','EL VINCULO','EL VINCULO',True,'2021-08-27'],['BOGOTA','TECHONOVIS','TECHO','TECHONOVIS',True,'2021-08-27'],['BOGOTA','TECHOVIP','TECHO','TECHOVIP',True,'2021-08-27'],['BOGOTA','TECHOVIS','TECHO','TECHOVIS',True,'2021-08-27'],['BOGOTA','MADPINE','MADRID PI„EROS','MADRID PI„EROS',True,'2021-08-27'],['BOGOTA','TUCANES','TUCANES','TUCANES',True,'2021-08-27'],['CARIBE','SANPABLO','VILLAS DE SAN PABLO','SAN PABLO',True,'2021-08-27'],['CARIBE','ALAM3','ALAMEDA DEL RIO','PELICANO',True,'2021-08-27'],['CARIBE','ALAMNO','ALAMEDA DEL RIO','PARDELA',True,'2021-08-27'],['CARIBE','ALAMVIS','ALAMEDA DEL RIO','PERDIZ',True,'2021-08-27'],['CARIBE','ALAMZ2','ALAMEDA DEL RIO','MZ 2',True,'2021-08-27'],['CARIBE','CDSALEGRIA','CIUDAD DE LOS SUE„OS','ALEGRIA',True,'2021-08-27'],['CARIBE','FELICIDAD','CIUDAD DE LOS SUE„OS','FELICIDAD',True,'2021-08-27'],['CARIBE','CDSMZ4-CAS','CIUDAD DE LOS SUE„OS','ARMONIA CASAS',True,'2021-08-27'],['CARIBE','CDSMZ4-TO','CIUDAD DE LOS SUE„OS','ARMONIA TORRES',True,'2021-08-27'],['CARIBE','CDSMZ5','CIUDAD DE LOS SUE„OS','VENTURA',True,'2021-08-27'],['CARIBE','CDSMZ3','CIUDAD DE LOS SUE„OS','CDS MZ3',True,'2021-08-27'],['CARIBE','HASANT','HACIENDA SAN ANTONIO','CAOBA',True,'2021-08-27'],['CARIBE','LOMA','LA LOMA','LA LOMA',True,'2021-08-27'],['CARIBE','MARBELLA','MARBELLA','MARBELLA',True,'2021-08-27'],['CARIBE','SITUM','IRATI','IRATI',True,'2021-08-27'],['CARIBE','SDM','SERENA DEL MAR','PORTELO',True,'2021-08-27'],['CARIBE','SERMAR','SERENA DEL MAR','PORTANOVA',True,'2021-08-27'],['CARIBE','SDMMZ4','SERENA DEL MAR','CASTELO',True,'2021-08-27'],['CARIBE','SDMMZ6','SERENA DEL MAR','SERENISIMA MZ6',True,'2021-08-27'],['CARIBE','CORAL11','CORAL','CORAL 11',True,'2021-08-27'],['CARIBE','CORAL6','CORAL','CORAL 6',True,'2021-08-27'],['CARIBE','BURECHE','BURECHE','BURECHE',True,'2021-08-27'],['CENTRO','GIRARDOT-MZ3','CIUDAD ESPLENDOR','INDIGO GIRARDOT MZ3',True,'2021-08-27'],['CENTRO','GIRARDOT-VIP','CIUDAD ESPLENDOR','TURQUESA',True,'2021-08-27'],['CENTRO','GIRARDOT-VIS','CIUDAD ESPLENDOR','CELESTE',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ14','ECOCIUDADES','CARMESI',True,'2021-08-27'],['CENTRO','IBAGUE-VIS','ECOCIUDADES','GRANATE',True,'2021-08-27'],['CENTRO','IBAGUE-VIP','ECOCIUDADES','CARMIN',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ11','ECOCIUDADES','IBAGUE VIP MZ11',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ12','ECOCIUDADES','IBAGUE VIP MZ12',True,'2021-08-27'],['CENTRO','VILLETA-VIS','CIUDAD CRISTALES','OPALO',True,'2021-08-27'],['CENTRO','VILLETA-VIP','CIUDAD CRISTALES','ZAFIRO',True,'2021-08-27'],['CENTRO','VILLETA-NOVIS','CIUDAD CRISTALES','AMBAR',True,'2021-08-27'],['CENTRO','CIUDADCRISTALES','CIUDAD CRISTALES','CIUDADCRISTALES',True,'2021-08-27']
        tbl_proyectos = pd.DataFrame(data, columns = ['tpr_regional', 'tpr_codigo_proyecto','tpr_macroproyecto','tpr_proyecto','tpr_estado','tpr_fecha_actualizacion'])
        tbl_proyectos = tbl_proyectos[tbl_proyectos['tpr_estado']==True]
    else:
        client = bigquery.Client()
        project_codes=stg_consolidado_corte.stg_codigo_proyecto.unique()
        text=""
        for project_code in project_codes:
            if text== "":
                text=text+"'"+project_code+"'"
            else:
                text=text+", '"+project_code+"'"
        query ="""
            SELECT *
                FROM `""" + BIGQUERY_ENVIRONMENT_NAME + """.""" + TBL_PROYECTOS + """`
                WHERE tpr_codigo_proyecto in ("""+ text +""")
                and tpr_estado = TRUE
            """

        #print(query)        
        tbl_proyectos= (client.query(query).result().to_dataframe(create_bqstorage_client=True,))
        #print(tbl_proyectos.head(5))

    # Verification: Is there information related to Building
    construction_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    construction_dataset=construction_dataset[construction_dataset['stg_area_prodesa']=='CS']
    building_report=False
    if construction_dataset[construction_dataset.columns[0]].count() == 0:
        building_report=False
    else:
        building_report=True

    print (building_report)
    # Verification: Is there information related to Planning
    planning_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    planning_dataset=planning_dataset[planning_dataset['stg_area_prodesa']=='PN']
    planning_report=False
    if planning_dataset[planning_dataset.columns[0]].count() == 0:
        planning_report=False
    else:
        planning_report=True

    print (planning_report)
    # Verification: Is there information related to Commercial
    commercial_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
    commercial_dataset=commercial_dataset[commercial_dataset['stg_area_prodesa']=='CL']
    commercial_report=False
    if commercial_dataset[commercial_dataset.columns[0]].count() == 0:
        commercial_report=False
    else:
        commercial_report=True

    print (commercial_report)

    print("  -Parametrization Script ending...");

    return tbl_proyectos, building_report, planning_report, commercial_report