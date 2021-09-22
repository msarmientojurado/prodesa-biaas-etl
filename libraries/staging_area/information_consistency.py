

def information_consistency(stg_consolidado_corte):

    continue_process = True

    #TODO:
    #Empty Fields verification in Cloumns
    #   *Column NAME - STRING
    #       - Insert the text "Actividad Nula" and report the issue
    #   *Column ACTUAL_START_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column ACTUAL_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column DURATION_REMAINED - STRING
    #       - Insert the text "1 dia" and report the issue
    #   *Column LIKELY_START_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column DURATION - STRING
    #       - Insert the text "1 dia" and report the issue
    #   *Column LIKELY_FINISH_DATE - DATE
    #       - Stop the process and report the issue
    #   *Column FIN_LINEA_BASE_EST - DATE
    #       - Stop the process and report the issue
    #   *Column D_START - DATE
    #       - Stop the process and report the issue
    #   *Column D_FINISH - DATE
    #       - Stop the process and report the issue
    #
    #           --------------------
    #
    #Validation: All the items are included in a list of valid items.
    #   *Column NOTE - STRING
    #       - Delete the values out of the reference set
    #
    #           --------------------
    #
    #Validation: Not null values, and values are just "Sí" or "No"
    #   *Column BUFFER - STRING
    #       - Stop the process and report the issue
    #   *Column TASK - STRING
    #       - Stop the process and report the issue
    #
    #           --------------------
    #
    #Validation: All the items are included in a list of valid items
    #   *PROJECT - STRING
    #       - Stop the process and report the issue

    return stg_consolidado_corte, continue_process