/* 01_filter_homogenize.pig - Procesa eventos individuales manteniendo toda la información */

-- Cargar los datos crudos desde el archivo TSV subido a HDFS.
-- El esquema debe coincidir con los datos exportados por export_mongo_to_hdfs.py
RAW_EVENTS = LOAD '/user/hadoop/waze_input/waze_events.tsv' USING PigStorage('\t') AS (
    event_id:chararray, 
    type:chararray, 
    address:chararray, 
    latitude:float, 
    longitude:float, 
    report_time:chararray, 
    reporter:chararray, 
    confidence:int
);

-- Filtrar registros válidos
FILTERED_EVENTS = FILTER RAW_EVENTS BY (type IS NOT NULL AND TRIM(type) != '') 
    AND (address IS NOT NULL AND TRIM(address) != '');

-- Enriquecer cada evento individual manteniendo toda la información original
INCIDENTS_ENRICHED = FOREACH FILTERED_EVENTS GENERATE
    event_id,
    type AS type_original,
    address,
    report_time,
    latitude,
    longitude,
    confidence,
    reporter,
    -- Extraer sector de la dirección (después de la última coma, limpiando espacios)
    TRIM(REGEX_EXTRACT(address, '([^,]+)$', 1)) AS sector,
    -- Extraer solo la calle (antes de la primera coma, limpiando espacios)  
    TRIM(REGEX_EXTRACT(address, '^([^,]+)', 1)) AS calle,
    -- Traducir tipos al español
    (CASE
        WHEN LOWER(type) MATCHES '.*hazard.*' THEN 'Peligro en Via'
        WHEN LOWER(type) MATCHES '.*jam.*' THEN 'Atasco de Trafico'
        WHEN LOWER(type) MATCHES '.*accident.*' THEN 'Accidente'
        WHEN LOWER(type) MATCHES '.*roadclosed.*' THEN 'Calle Cerrada'
        ELSE 'Otro'
    END) AS tipo_evento_es,
    -- Extraer hora del reporte
    REGEX_EXTRACT(report_time, '(\\d{2}):', 1) AS hora_reporte;

-- Eliminar el directorio de salida anterior para evitar errores.
rmf /user/hadoop/waze_processed/individual_events;

-- Guardar TODOS los eventos individuales enriquecidos
STORE INCIDENTS_ENRICHED INTO '/user/hadoop/waze_processed/individual_events' USING PigStorage('\t');