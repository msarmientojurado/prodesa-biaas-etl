
DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_inicio_venta;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_inicio_venta
(
    tiv_regional STRING NOT NULL,
    tiv_codigo_proyecto STRING NOT NULL,
    tiv_macroproyecto STRING NOT NULL,
    tiv_proyecto STRING NOT NULL,
    tiv_etapa STRING NOT NULL,
    tiv_dias_atraso INT64,
    tiv_inicio_ventas_proyectado DATE,
    tiv_inicio_ventas_programado DATE,
    tiv_fiv_proyectado DATE,
    tiv_fiv_programado DATE,
    tiv_ppto_revisado_proyectado DATE,
    tiv_ppto_revisado_programado DATE,
    tiv_docs_ppto_proyectado DATE,
    tiv_docs_ppto_programado DATE,
    tiv_encargo_fiduciario_proyectado DATE,
    tiv_encargo_fiduciario_programado DATE,
    tiv_kit_comercial_proyectado DATE,
    tiv_kit_comercial_programado DATE,
    tiv_val_sv_model_proyectado DATE,
    tiv_val_sv_model_programado DATE,
    tiv_const_sv_model_proyectado DATE,
    tiv_const_sv_model_programado DATE,
    tiv_aprobac_lc_proyectado DATE,
    tiv_aprobacion_lc_programado DATE,
    tiv_radicacion_lc_proyectado DATE,
    tiv_radicacion_lc_programado DATE,
    tiv_salida_ventas_proyectado DATE,
    tiv_salida_ventas_programado DATE,
    tiv_acta_constituc_proyectado DATE,
    tiv_acta_constituc_programado DATE,
    tiv_visto_bueno_proyectado DATE,
    tiv_visto_bueno_programado DATE,
    tiv_elab_alternativ_proyectado DATE,
    tiv_elab_alternativ_programado DATE,
    tiv_prod_objetivo_proyectado DATE,
    tiv_prod_objetivo_programado DATE,
    tiv_lluvia_ideas_proyectado DATE,
    tiv_lluvia_ideas_programado DATE,
    tiv_kit_desarrollos_proyectado DATE,
    tiv_kit_desarrollos_programado DATE,
    tiv_fecha_corte DATE NOT NULL,
    tiv_fecha_proceso DATE NOT NULL,
    tiv_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_inicio_promesa;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_inicio_promesa
(
    tip_regional STRING NOT NULL,
    tip_codigo_proyecto STRING NOT NULL,
    tip_macroproyecto STRING NOT NULL,
    tip_proyecto STRING NOT NULL,
    tip_etapa STRING NOT NULL,
    tip_dias_atraso INT64,
    tip_inicio_promesas_proyectado DATE,
    tip_inicio_promesas_programado DATE,
    tip_ent_kit_prom_proyectado DATE,
    tip_ent_kit_prom_programado DATE,
    tip_permiso_ventas_proyectado DATE,
    tip_permiso_ventas_programado DATE,
    tip_min_hipo_reg_proyectado DATE,
    tip_min_hipo_reg_programado DATE,
    tip_rad_min_hipo_reg_proyectado DATE,
    tip_rad_min_hipo_reg_programado DATE,
    tip_cred_construct_proyectado DATE,
    tip_cred_construct_programado DATE,
    tip_linderos_proyectado DATE,
    tip_linderos_programado DATE,
    tip_fai_proyectado DATE,
    tip_fai_programado DATE,
    tip_constitut_urban_proyectado DATE,
    tip_constitut_urban_programado DATE,
    tip_fecha_corte DATE NOT NULL,
    tip_fecha_proceso DATE NOT NULL,
    tip_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_inicio_construccion;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_inicio_construccion
(
    tic_regional STRING NOT NULL,
    tic_codigo_proyecto STRING NOT NULL,
    tic_macroproyecto STRING NOT NULL,
    tic_proyecto STRING NOT NULL,
    tiC_etapa STRING NOT NULL,
    tic_dias_atraso INT64,
    tic_inicio_construccion_proyectado DATE,
    tic_inicio_construccion_programado DATE,
    tic_proc_contratacion_proyectado DATE,
    tic_proc_contratacion_programado DATE,
    tic_entrega_kit2_proyectado DATE,
    tic_entrega_kit2_programado DATE,
    tic_entrega_kit1_proyectado DATE,
    tic_entrega_kit1_programado DATE,
    tic_factib_inic_constru_proyectado DATE,
    tic_factib_inic_constru_programado DATE,
    tic_ppto_definitivo_proyectado DATE,
    tic_ppto_definitivo_programado DATE,
    tic_ppto_sipro_proyectado DATE,
    tic_ppto_sipro_programado DATE,
    tic_docs_inic_constru_proyectado DATE,
    tic_docs_inic_constru_programado DATE,
    tic_diseno_inic_constru_proyectado DATE,
    tic_diseno_inic_constru_programado DATE,
    tic_fecha_corte DATE NOT NULL,
    tic_fecha_proceso DATE NOT NULL,
    tic_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_inicio_escrituracion;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_inicio_escrituracion
(
    tie_regional STRING NOT NULL,
    tie_codigo_proyecto STRING NOT NULL,
    tie_macroproyecto STRING NOT NULL,
    tie_proyecto STRING NOT NULL,
    tie_etapa STRING NOT NULL,
    tie_dias_atraso INT64,
    tie_inicio_escrituracion_proyectado DATE,
    tie_inicio_escrituracion_programado DATE,
    tie_poder_fiduciaria_proyectado DATE,
    tie_poder_fiduciaria_programado DATE,
    tie_salida_rph_proyectado DATE,
    tie_salida_rph_programado DATE,
    tie_cierre_rph_proyectado DATE,
    tie_cierre_rph_programado DATE,
    tie_licencia_ph_proyectado DATE,
    tie_licencia_ph_programado DATE,
    tie_modificacion_lc_proyectado DATE,
    tie_modificacion_lc_programado DATE,
    tie_radic_modif_lc_proyectado DATE,
    tie_radic_modif_lc_programado DATE,
    tie_fecha_corte DATE NOT NULL,
    tie_fecha_proceso DATE NOT NULL,
    tie_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_proyectos_planeacion;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_proyectos_planeacion
(
    tpp_regional STRING NOT NULL,
    tpp_codigo_proyecto STRING NOT NULL,
    tpp_macroproyecto STRING NOT NULL,
    tpp_proyecto STRING NOT NULL,
    tpp_hito STRING,
    tpp_etapa STRING,
    tpp_tarea_consume_buffer STRING,
    tpp_avance_cc FLOAT64,
    tpp_avance_comparativo_semana INT64,
    tpp_consumo_buffer FLOAT64,
    tpp_consumo_buffer_color INT64,
    tpp_consumo_buffer_comparativo INT64,
    tpp_fin_proyectado_optimista DATE,
    tpp_fin_proyectado_pesimista DATE,
    tpp_fin_programada DATE,
    tpp_dias_atraso INT64,
    tpp_ultima_semana FLOAT64,
    tpp_ultimo_mes FLOAT64,
    tpp_fecha_corte DATE NOT NULL,
    tpp_fecha_proceso DATE NOT NULL,
    tpp_lote_proceso INT64 NOT NULL
);


DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_proyectos_construccion;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_proyectos_construccion
(
    tpc_regional STRING NOT NULL,
    tpc_codigo_proyecto STRING NOT NULL,
    tpc_macroproyecto STRING NOT NULL,
    tpc_proyecto STRING NOT NULL,
    tpc_etapa STRING NOT NULL,
    tpc_programacion STRING NOT NULL,
    tpc_tarea_consume_buffer STRING,
    tpc_avance_cc FLOAT64,
    tpc_avance_comparativo_semana INT64,
    tpc_consumo_buffer FLOAT64,
    tpc_consumo_buffer_color INT64,
    tpc_consumo_buffer_comparativo int64,
    tpc_fin_proyectado_optimista DATE,
    tpc_fin_proyectado_pesimista DATE,
    tpc_fin_programada DATE,
    tpc_dias_atraso INT64,
    tpc_ultima_semana FLOAT64,
    tpc_ultimo_mes FLOAT64,
    tpc_fecha_corte DATE NOT NULL,
    tpc_fecha_proceso DATE NOT NULL,
    tpc_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_proyectos_comercial;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_proyectos_comercial
(
    tpcm_regional STRING NOT NULL,
    tpcm_codigo_proyecto STRING NOT NULL,
    tpcm_macroproyecto STRING NOT NULL,
    tpcm_proyecto STRING NOT NULL,
    tpcm_etapa STRING NOT NULL,
    tpcm_programacion STRING NOT NULL,
    tpcm_tarea_consume_buffer STRING,
    tpcm_avance_cc FLOAT64,
    tpcm_avance_comparativo_semana INT64,
    tpcm_consumo_buffer FLOAT64,
    tpcm_consumo_buffer_color INT64,
    tpcm_consumo_buffer_comparativo int64,
    tpcm_fin_proyectado_optimista DATE,
    tpcm_fin_proyectado_pesimista DATE,
    tpcm_fin_programada DATE,
    tpcm_dias_atraso INT64,
    tpcm_ultima_semana FLOAT64,
    tpcm_ultimo_mes FLOAT64,
    tpcm_fecha_corte DATE NOT NULL,
    tpcm_fecha_proceso DATE NOT NULL,
    tpcm_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_reporte_por_entrega;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_reporte_por_entrega
(
    trpe_regional STRING NOT NULL,
    trpe_codigo_proyecto STRING NOT NULL,
    trpe_macroproyecto STRING NOT NULL,
    trpe_proyecto STRING NOT NULL,
    trpe_programacion STRING,
    trpe_tarea_entrega STRING,
    trpe_etapa STRING,
    trpe_entrega_real DATE,
    trpe_entrega_programada DATE,
    trpe_fecha_corte DATE NOT NULL,
    trpe_fecha_proceso DATE NOT NULL,
    trpe_lote_proceso INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_proyectos;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_proyectos
(
    tpr_regional STRING NOT NULL,
    tpr_codigo_proyecto STRING NOT NULL,
    tpr_macroproyecto STRING NOT NULL,
    tpr_proyecto STRING NOT NULL,
    tpr_estado BOOL NOT NULL,
    tpr_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_valores_hitos;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_valores_hitos
(
    tvh_sigla STRING NOT NULL,
    tvh_hito STRING NOT NULL,
    tvh_estado BOOL NOT NULL,
    tvh_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_control_cargue;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_control_cargue
(
    tcc_nombre_fuente STRING NOT NULL,
    tcc_nombre_backup STRING NOT NULL,
    tcc_fecha_proceso DATE NOT NULL,
    tcc_lote_proceso INT64 NOT NULL,
    tcc_cantidad_registros INT64 NOT NULL
);
INSERT INTO `proyecto-prodesa.modelo_biaas_python_test.tbl_control_cargue`
VALUES ("ghost", "ghost", DATE "2020-12-12", 0, 0)

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_area_prodesa;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_area_prodesa
(
    tap_nombre_area STRING NOT NULL,
    tap_sigla_area STRING NOT NULL,
    tap_estado BOOL NOT NULL,
    tap_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_consolidado_corte;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_consolidado_corte
(
    tcco_project_id INT64 NOT NULL,
    tcco_wbs STRING NOT NULL,
    tcco_nombre_actividad STRING NOT NULL,
    tcco_fecha_inicial_actual DATE,
    tcco_fecha_final_actual DATE,
    tcco_duracion_real_cantidad FLOAT64,
    tcco_duracion_real_unidad STRING,
    tcco_fecha_inicio_planeada DATE,
    tcco_duracion_cantidad INT64,
    tcco_duracion_unidad STRING,
    tcco_riesgo_bajo_duracion_cantidad INT64,
    tcco_riesgo_bajo_duracion_unidad STRING,
    tcco_recursos STRING,
    tcco_actividad_sucesora STRING,
    tcco_impacto_buffer INT64,
    tcco_indicador_cantidad INT64,
    tcco_indicador_unidad STRING,
    tcco_porc_completitud FLOAT64,
    tcco_ind_buffer STRING,
    tcco_indice_buffer STRING,
    tcco_duracion_critica_cantidad FLOAT64,
    tcco_duracion_critica_unidad STRING,
    tcco_ind_tarea STRING,
    tcco_ind_tarea_critica STRING,
    tcco_estado INT64,
    tcco_numero_esquema STRING,
    tcco_fecha_fin_planeada DATE,
    tcco_area_prodesa STRING NOT NULL,
    tcco_nombre_archivo STRING NOT NULL,
    tcco_codigo_proyecto STRING NOT NULL,
    tcco_etapa_proyecto STRING NOT NULL,
    tcco_programacion_proyecto STRING NOT NULL,
    tcco_fecha_corte DATE NOT NULL,
    tcco_fecha_inicial DATETIME,
    tcco_fecha_fin DATETIME,
    tcco_actividad_predecesora STRING,
    tcco_notas STRING,
    tcco_duracion_restante_cantidad FLOAT64,
    tcco_duracion_restante_unidad STRING,
    tcco_fecha_fin_programada DATE,
    tcco_fecha_proceso DATE NOT NULL,
    tcco_lote_proceso INT64 NOT NULL
); 

DROP TABLE IF EXISTS modelo_biaas_python_test.tbl_graficos_tiempo_avance_buffer;
CREATE TABLE IF NOT EXISTS modelo_biaas_python_test.tbl_graficos_tiempo_avance_buffer
(
    tgabt_area_prodesa  STRING NOT NULL,
    tgabt_regional  STRING NOT NULL,
    tgabt_codigo_proyecto   STRING NOT NULL,
    tgabt_macroproyecto STRING NOT NULL,
    tgabt_proyecto  STRING NOT NULL,
    tgabt_etapa STRING NOT NULL,
    tgabt_programacion STRING NOT NULL,
    tgabt_avance_cc     FLOAT64,
    tgabt_consumo_buffer    FLOAT64,
    tgabt_fecha_inicio_linea_base   DATE NOT NULL,
    tgabt_fecha_fin_linea_base  DATE NOT NULL,
    tgabt_fecha_fin_buffer_linea_base   DATE NOT NULL,
    tgabt_fecha_corte   DATE NOT NULL,
    tgabt_fecha_proceso     DATE NOT NULL,
    tgabt_lote_proceso  INT64 NOT NULL,
); 