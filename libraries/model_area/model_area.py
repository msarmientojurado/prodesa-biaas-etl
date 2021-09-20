
from libraries.model_area.planning.mdl_ar_mlstns_planning import mdl_ar_mlstns_planning
from libraries.model_area.milestones.phases.mdl_ar_mlstns_inicio_construccion import mdl_ar_mlstns_inicio_construccion
from libraries.settings import ENVIRONMENT


def model(tmp_proyectos_construccion, tmp_proyectos_planeacion):

    print(" *Model Starting...")

    if ENVIRONMENT == "Production":
        #TODO
        #   1. "Control de Hitos de Planeacion"
        #   4. "Consolidado de Proyectos de Comercial"
        #   5. "Reporte por entrega"
        
        #"Consolidado de Proyectos de Construccion"
        mdl_ar_mlstns_inicio_construccion(tmp_proyectos_construccion)

        #"Consolidado de Proyectos de Planeacion"
        mdl_ar_mlstns_planning(tmp_proyectos_planeacion)

        
    print(" *Model ending...")