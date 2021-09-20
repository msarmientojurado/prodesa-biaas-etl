

from libraries.model_area.milestones.phases.mdl_ar_mlstns_inicio_promesa import mdl_ar_mlstns_inicio_promesa
from libraries.model_area.milestones.phases.mdl_ar_mlstns_inicio_venta import mdl_ar_mlstns_inicio_venta


def model_milestones(tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion):
    #TODO
    #   4. Operate over tbl_inicio_escrituracion

    #Operate over tbl_inicio_venta
    mdl_ar_mlstns_inicio_venta(tbl_inicio_venta)

    #Operate over tbl_inicio_promesa
    mdl_ar_mlstns_inicio_promesa(tbl_inicio_promesa)

    #Operate over tbl_inicio_construccion
    #mdl_ar_mlstns_inicio_construccion(tmp_proyectos_construccion)

    return
