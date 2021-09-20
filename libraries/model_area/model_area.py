
from libraries.model_area.milestones.phases.mdl_ar_mlstns_inicio_construccion import mdl_ar_mlstns_inicio_construccion
from libraries.settings import ENVIRONMENT


def model(tmp_proyectos_construccion):

    print(" *Model Starting...")

    if ENVIRONMENT == "Production":
        #TODO
        #   1. "Control de Hitos de Planeacion"
        #   2. "Consolidado de Proyectos de Planeacion"
        #   4. "Consolidado de Proyectos de Comercial"
        #   5. "Reporte por entrega"
        
        #"Consolidado de Proyectos de Construccion"
        mdl_ar_mlstns_inicio_construccion(tmp_proyectos_construccion)

        
    print(" *Model ending...")