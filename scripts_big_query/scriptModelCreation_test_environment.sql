DROP TABLE IF EXISTS modelo_biaas.tbl_inicio_venta;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_inicio_venta
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

DROP TABLE IF EXISTS modelo_biaas.tbl_inicio_promesa;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_inicio_promesa
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

DROP TABLE IF EXISTS modelo_biaas.tbl_inicio_construccion;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_inicio_construccion
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

DROP TABLE IF EXISTS modelo_biaas.tbl_inicio_escrituracion;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_inicio_escrituracion
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

DROP TABLE IF EXISTS modelo_biaas.tbl_proyectos_planeacion;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_proyectos_planeacion
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


DROP TABLE IF EXISTS modelo_biaas.tbl_proyectos_construccion;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_proyectos_construccion
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

DROP TABLE IF EXISTS modelo_biaas.tbl_proyectos_comercial;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_proyectos_comercial
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

DROP TABLE IF EXISTS modelo_biaas.tbl_reporte_por_entrega;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_reporte_por_entrega
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

DROP TABLE IF EXISTS modelo_biaas.tbl_proyectos;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_proyectos
(
    tpr_regional STRING NOT NULL,
    tpr_codigo_proyecto STRING NOT NULL,
    tpr_macroproyecto STRING NOT NULL,
    tpr_proyecto STRING NOT NULL,
    tpr_estado BOOL NOT NULL,
    tpr_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas.tbl_valores_hitos;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_valores_hitos
(
    tvh_sigla STRING NOT NULL,
    tvh_hito STRING NOT NULL,
    tvh_estado BOOL NOT NULL,
    tvh_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas.tbl_control_cargue;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_control_cargue
(
    tcc_nombre_fuente STRING NOT NULL,
    tcc_nombre_backup STRING NOT NULL,
    tcc_fecha_proceso DATE NOT NULL,
    tcc_lote_proceso INT64 NOT NULL,
    tcc_cantidad_registros INT64 NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas.tbl_area_prodesa;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_area_prodesa
(
    tap_nombre_area STRING NOT NULL,
    tap_sigla_area STRING NOT NULL,
    tap_estado BOOL NOT NULL,
    tap_fecha_actualizacion DATE NOT NULL
);

DROP TABLE IF EXISTS modelo_biaas.tbl_consolidado_corte;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_consolidado_corte
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

DROP TABLE IF EXISTS modelo_biaas.tbl_graficos_tiempo_avance_buffer;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_graficos_tiempo_avance_buffer
(
    tgabt_area_prodesa  STRING NOT NULL,
    tgabt_regional  STRING NOT NULL,
    tgabt_codigo_proyecto   STRING NOT NULL,
    tgabt_macroproyecto STRING NOT NULL,
    tgabt_proyecto  STRING NOT NULL,
    tgabt_etapa STRING NOT NULL,
    tgabt_programacion STRING NOT NULL,
    tgabt_avance_cc     FLOAT64 NOT NULL,
    tgabt_consumo_buffer    FLOAT64 NOT NULL,
    tgabt_fecha_inicio_linea_base   DATE NOT NULL,
    tgabt_fecha_fin_linea_base  DATE NOT NULL,
    tgabt_fecha_fin_buffer_linea_base   DATE NOT NULL,
    tgabt_fecha_corte   DATE NOT NULL,
    tgabt_fecha_proceso     DATE NOT NULL,
    tgabt_lote_proceso  INT64 NOT NULL,
);

DROP TABLE IF EXISTS modelo_biaas.tbl_descarga_reportes;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_descarga_reportes
(
    tdr_enlace_descarga  STRING NOT NULL,
    tdr_fecha_corte   DATE NOT NULL,
    tdr_fecha_proceso     DATE NOT NULL,
    tdr_lote_proceso  INT64 NOT NULL,
);

DROP TABLE IF EXISTS modelo_biaas.tbl_mapeo_programacion;
CREATE TABLE IF NOT EXISTS modelo_biaas.tbl_mapeo_programacion
(
    tmp_codigo              STRING NOT NULL,
    tmp_nombre              STRING NOT NULL,
    tmp_estado              BOOL NOT NULL,
    tmp_fecha_actualizacion DATE NOT NULL,
);

INSERT INTO modelo_biaas.tbl_control_cargue(tcc_nombre_fuente,tcc_nombre_backup,tcc_fecha_proceso, tcc_lote_proceso, tcc_cantidad_registros
)
VALUES ("ghost", "ghost", DATE "2020-12-12", 0, 0);

-------------------------------------------------------------------
---                             TABLA PROGRAMACIONES
-------------------------------------------------------------------
INSERT INTO modelo_biaas.tbl_mapeo_programacion(tmp_codigo,tmp_nombre,tmp_estado,tmp_fecha_actualizacion
)
VALUES
('CA','CASA',true,'2021-11-11'),
('DOTACION','DOTACION',true,'2021-11-11'),
('EDF-PARQUEADEROS','PAQUEADEROS',true,'2021-11-11'),
('PARQUEADERO','PAQUEADEROS',true,'2021-11-11'),
('PARQU','PAQUEADEROS',true,'2021-11-11'),
('PORTERIA','PORTERIA',true,'2021-11-11'),
('TANQUE','TANQUE',true,'2021-11-11'),
('TO','TORRE',true,'2021-11-11'),
('UI','URBANISMO INTERNO',true,'2021-11-11'),
('UE','URBANISMO EXTERNO',true,'2021-11-11'),
('ZC','ZONAS COMUNES',true,'2021-11-11'),
('ZC-BASURAS','CUARTO DE BASURAS',true,'2021-11-11'),
('ZC-CLUB','CLUB HOUSE',true,'2021-11-11'),
('ZC-CUARTOEL','CUARTO ELECTRICO',true,'2021-11-11'),
('ZC-ESTRUCTURA','ESTRUCTURA DE DISIPACION',true,'2021-11-11'),
('ZC-PUENTE','PUENTE',true,'2021-11-11');

------------------------------------
---                             TABLA DE PROYECTOS
-----------------------------------------------------------------
INSERT INTO modelo_biaas.tbl_proyectos(tpr_regional,tpr_codigo_proyecto,
tpr_macroproyecto,
tpr_proyecto,
tpr_estado,
tpr_fecha_actualizacion)
VALUES
('CALI','PINTURAS','PINTURAS','PINTURAS COLORS',true,'2021-08-27'),
('CALI','SANTABARB','SANTABARBARA','SANTABARBARA',true,'2021-08-27'),
('CALI','PASCUAL','PASCUAL','PASCUAL',true,'2021-08-27'),
('CALI','TRIANGULO','TRIANGULO','ATRIO DE  PANCE',true,'2021-08-27'),
('BOGOTA','MADNV','ALTOS DE MADELENA','MADELENA',true,'2021-08-27'),
('BOGOTA','AMERICAN-PIPE-NOVIS','AMERICAN PIPE','AMERICAN PIPE NOVIS',true,'2021-08-27'),
('BOGOTA','AMERICAN-PIPE-VIP','AMERICAN PIPE','AMERICAN PIPE VIP',true,'2021-08-27'),
('BOGOTA','AMERICAN-PIPE-VIS','AMERICAN PIPE','AMERICAN PIPE VIS',true,'2021-08-27'),
('BOGOTA','CALLE13','CALLE13','CALLE13',true,'2021-08-27'),
('BOGOTA','CHANCO','CHANCO','CHANCO',true,'2021-08-27'),
('BOGOTA','URDECO','CIPRES DE LA FLORIDA','CIPRES DE LA FLORIDA',true,'2021-08-27'),
('BOGOTA','CVM55TV1','CIUDAD VERDE','YERBABUENA',true,'2021-08-27'),
('BOGOTA','SMART2','EQUILIBRIUM','EQUILIBRIUM',true,'2021-08-27'),
('BOGOTA','BELLAFLORA','BELLAFLORA','BELLAFLORA',true,'2021-08-27'),
('BOGOTA','SNJORLCER','HACIENDA ALCALA','CEREZO',true,'2021-08-27'),
('BOGOTA','SNJORROB','HACIENDA ALCALA','ROBLE',true,'2021-08-27'),
('BOGOTA','SNJORL','HACIENDA ALCALA','SAUCE',true,'2021-08-27'),
('BOGOTA','SNJORLT1','HACIENDA ALCALA','SAUCE TO1',true,'2021-08-27'),
('BOGOTA','SNJORLT4','HACIENDA ALCALA','SAUCE TO4',true,'2021-08-27'),
('BOGOTA','SNJORLT9','HACIENDA ALCALA','SAUCE TO9',true,'2021-08-27'),
('BOGOTA','SNJORLSAM','HACIENDA ALCALA','SAMAN',true,'2021-08-27'),
('BOGOTA','PDAW2TW','PALO DE AGUA','KATIOS',true,'2021-08-27'),
('BOGOTA','PDAAISB','PALO DE AGUA','IGUAQUE B',true,'2021-08-27'),
('BOGOTA','PDAPAW','PALO DE AGUA','MACARENA',true,'2021-08-27'),
('BOGOTA','PDASWTW','PALO DE AGUA','PALO DE AGUA',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ10','CIUDADELA FORESTA','MILANO',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ7','CIUDADELA FORESTA','IBIZ',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ2','CIUDADELA FORESTA','AMAZILIA',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ8','CIUDADELA FORESTA','ANDARRIOS',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ4','CIUDADELA FORESTA','CIUDADELA FORESTA MZ4',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ5','CIUDADELA FORESTA','TANGARA',true,'2021-08-27'),
('BOGOTA','PRAFU-MZ3','CIUDADELA FORESTA','CIUDADELA FORESTA MZ3',true,'2021-08-27'),
('BOGOTA','PRAFU-UE','CIUDADELA FORESTA','CIUDADELA FORESTA UE',true,'2021-08-27'),
('BOGOTA','RECREO','RECREO','RECREO',true,'2021-08-27'),
('BOGOTA','SOLEM5','RESERVA DE MADRID','PALERMO',true,'2021-08-27'),
('BOGOTA','SOLEM8','RESERVA DE MADRID','PAMPLONA',true,'2021-08-27'),
('BOGOTA','SNHILARIO','SAN HILARIO','SAN HILARIO',true,'2021-08-27'),
('BOGOTA','SNLUIS','SAN LUIS','SAN LUIS',true,'2021-08-27'),
('BOGOTA','VINCULO','EL VINCULO','EL VINCULO',true,'2021-08-27'),
('BOGOTA','TECHONOVIS','TECHO','TECHONOVIS',true,'2021-08-27'),
('BOGOTA','TECHOVIP','TECHO','TECHOVIP',true,'2021-08-27'),
('BOGOTA','TECHOVIS','TECHO','TECHOVIS',true,'2021-08-27'),
('BOGOTA','MADPINE','MADRID PI„EROS','MADRID PI„EROS',true,'2021-08-27'),
('BOGOTA','TUCANES','TUCANES','BALCONES DEL SOL',true,'2021-08-27'),
('CARIBE','SANPABLO','VILLAS DE SAN PABLO','SAN PABLO',true,'2021-08-27'),
('CARIBE','ALAM3','ALAMEDA DEL RIO','PELICANO',true,'2021-08-27'),
('CARIBE','ALAMNO','ALAMEDA DEL RIO','PARDELA',true,'2021-08-27'),
('CARIBE','ALAMVIS','ALAMEDA DEL RIO','PERDIZ',true,'2021-08-27'),
('CARIBE','ALAMZ2','ALAMEDA DEL RIO','ALAMEDA MZ2',true,'2021-08-27'),
('CARIBE','CDSALEGRIA','CIUDAD DE LOS SUE„OS','ALEGRIA',true,'2021-08-27'),
('CARIBE','FELICIDAD','CIUDAD DE LOS SUE„OS','FELICIDAD',true,'2021-08-27'),
('CARIBE','CDSMZ4-CAS','CIUDAD DE LOS SUE„OS','ARMONIA CASAS',true,'2021-08-27'),
('CARIBE','CDSMZ4-TO','CIUDAD DE LOS SUE„OS','ARMONIA TORRES',true,'2021-08-27'),
('CARIBE','CDSMZ5','CIUDAD DE LOS SUEÑOS','VENTURA',true,'2021-08-27'),
('CARIBE','CDSMZ3','CIUDAD DE LOS SUEÑOS','CIUDAD DE LOS SUEÑOS MZ3',true,'2021-08-27'),
('CARIBE','HASANT','HACIENDA SAN ANTONIO','CAOBA',true,'2021-08-27'),
('CARIBE','LOMA','LA LOMA','LA LOMA',true,'2021-08-27'),
('CARIBE','MARBELLA','MARBELLA','MARBELLA',true,'2021-08-27'),
('CARIBE','SITUM','IRATI','IRATI',true,'2021-08-27'),
('CARIBE','SDM','SERENA DEL MAR','PORTELO',true,'2021-08-27'),
('CARIBE','SERMAR','SERENA DEL MAR','PORTANOVA',true,'2021-08-27'),
('CARIBE','SDMMZ4','SERENA DEL MAR','CASTELO',true,'2021-08-27'),
('CARIBE','SDMMZ6','SERENA DEL MAR','SERENISIMA MZ6',true,'2021-08-27'),
('CARIBE','CORAL11','CORAL','CORAL 11',true,'2021-08-27'),
('CARIBE','CORAL6','CORAL','CORAL 6',true,'2021-08-27'),
('CARIBE','BURECHE','BURECHE','BURECHE',true,'2021-08-27'),
('CENTRO','GIRARDOT-MZ3','CIUDAD ESPLENDOR','INDIGO GIRARDOT MZ3',true,'2021-08-27'),
('CENTRO','GIRARDOT-VIP','CIUDAD ESPLENDOR','TURQUESA',true,'2021-08-27'),
('CENTRO','GIRARDOT-VIS','CIUDAD ESPLENDOR','CELESTE',true,'2021-08-27'),
('CENTRO','IBAGUE-VIPMZ14','ECOCIUDADES','CARMESI',true,'2021-08-27'),
('CENTRO','IBAGUE-VIS','ECOCIUDADES','GRANATE',true,'2021-08-27'),
('CENTRO','IBAGUE-VIP','ECOCIUDADES','CARMIN',true,'2021-08-27'),
('CENTRO','IBAGUE-VIPMZ11','ECOCIUDADES','ESCARLATA',true,'2021-08-27'),
('CENTRO','IBAGUE-VIPMZ12','ECOCIUDADES','BERMELLON',true,'2021-08-27'),
('CENTRO','VILLETA-VIS','CIUDAD CRISTALES','OPALO',true,'2021-08-27'),
('CENTRO','VILLETA-VIP','CIUDAD CRISTALES','ZAFIRO',true,'2021-08-27'),
('CENTRO','VILLETA-NOVIS','CIUDAD CRISTALES','AMBAR',true,'2021-08-27'),
('CENTRO','CIUDADCRISTALES','CIUDAD CRISTALES','CIUDADCRISTALES',true,'2021-08-27'),
('CENTRO','ECOCIUDADES','ECOCIUDADES','ECOCIUDADES',true,'2021-08-27');

----------------------------------------------------------
---                             TABLA DE HITOS
----------------------------------------------------------

INSERT INTO modelo_biaas.tbl_valores_hitos(tvh_sigla,tvh_hito,tvh_estado,tvh_fecha_actualizacion
)
VALUES
('IV','Inicio Ventas',true,'2021-08-26'),
('IP','Inicio Promesas',true,'2021-08-26'),
('IC','Inicio Construccion',true,'2021-08-28'),
('IE','Inicio de Escrituracion',true,'2021-08-28'),
('DC','Desenglobe Catastral',true,'2021-08-28'),
('SP','Servicio Publico',true,'2021-08-28'),
('GAS','GAS',true,'2021-08-28'),
('AC','ACUEDUCTO',true,'2021-08-28'),
('EL','ELECTRICIDAD',true,'2021-08-28'),
('GASUE','GASODUCTO',true,'2021-08-28');

----------------------------------------------------------
---                             TABLA DE AREAS
----------------------------------------------------------
INSERT INTO modelo_biaas.tbl_area_prodesa(tap_nombre_area, tap_sigla_area,tap_estado, tap_fecha_actualizacion
)
VALUES
('PLANEACION','PN',true,'2021-08-21'),
('COMERCIAL','CL',true,'2021-08-21'),
('CONSTRUCCION','CS',true,'2021-08-21');

------------------------------------------------------------