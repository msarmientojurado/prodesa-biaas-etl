

from libraries.temporary_area.milestones.phases.tmp_ar_mlstns_inicio_venta import tmp_ar_mlstns_inicio_venta
from libraries.temporary_area.milestones.phases.tmp_ar_mlstns_inicio_promesa import tmp_ar_mlstns_inicio_promesa
from libraries.temporary_area.milestones.phases.tmp_ar_mlstns_inicio_escrituracion import tmp_ar_mlstns_inicio_escrituracion
from libraries.temporary_area.milestones.phases.tmp_ar_mlstns_inicio_construccion import tmp_ar_mlstns_inicio_construccion

import pandas as pd


def tmp_ar_milestones(stg_consolidado_corte, tbl_proyectos, current_bash):
    
    print("  *Milestones Starting")

    #Lets start by building the dataset to work, which includes those registers 
    # which have the words `IV`, `IP`,`IC`,`IE` in their `stg_programacion_proyecto` 
    # column


    #list_of_phases = ["IV", "PL"]
    milestones_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_duracion_cantidad', 'stg_fecha_fin_planeada', 'stg_ind_buffer', 'stg_fecha_fin','stg_nombre_actividad', 'stg_fecha_corte', 'stg_notas', 'stg_fecha_final_actual', 'stg_fin_linea_base_estimado')]
    milestones_dataset['key']=milestones_dataset['stg_codigo_proyecto']+'_'+milestones_dataset['stg_etapa_proyecto']

    tbl_inicio_venta = tmp_ar_mlstns_inicio_venta(milestones_dataset, tbl_proyectos, current_bash)

    tbl_inicio_promesa=tmp_ar_mlstns_inicio_promesa(milestones_dataset, tbl_proyectos, current_bash)

    tbl_inicio_construccion=tmp_ar_mlstns_inicio_construccion(milestones_dataset, tbl_proyectos, current_bash)

    tbl_inicio_escrituracion=tmp_ar_mlstns_inicio_escrituracion(milestones_dataset, tbl_proyectos, current_bash)

    print("  *Milestones ending")

    return tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion




