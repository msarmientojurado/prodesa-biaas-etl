
from libraries.model_area.building.mdl_ar_building import mdl_ar_building
from libraries.model_area.planning.mdl_ar_planning import mdl_ar_planning
from libraries.model_area.milestones.mdl_ar_milestones import model_milestones
from libraries.settings import ENVIRONMENT


def model(tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion, tmp_proyectos_construccion, tmp_proyectos_planeacion):

    print(" *Model Starting...")

    if ENVIRONMENT == "Production":
        #TODO
        #   4. "Consolidado de Proyectos de Comercial"
        #   5. "Reporte por entrega"
        
        #"Consolidado de Proyectos de Construccion"
        mdl_ar_building(tmp_proyectos_construccion)

        #"Consolidado de Proyectos de Planeacion"
        mdl_ar_planning(tmp_proyectos_planeacion)

        #"Control de Hitos de Planeacion"
        model_milestones(tbl_inicio_venta, tbl_inicio_promesa, tbl_inicio_construccion, tbl_inicio_escrituracion)

        
    print(" *Model ending...")