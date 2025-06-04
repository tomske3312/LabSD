-- Cargar las JARs del conector MongoDB Hadoop
REGISTER /opt/pig-0.17.0/lib/mongo-hadoop-core-2.0.2.jar;
REGISTER /opt/pig-0.17.0/lib/mongo-hadoop-pig-2.0.2.jar;
REGISTER /opt/pig-0.17.0/lib/mongo-java-driver-3.12.11.jar;
REGISTER /opt/pig-0.17.0/lib/json-simple-1.1.1.jar; -- Necesario para JsonStorage en algunas configuraciones

-- Cargar datos de HDFS (TSV)
-- El scraper ahora genera un event_id con un timestamp, lo que hace que cada "scrape" sea único.
-- El importador a MongoDB utiliza upsert basado en el event_id del scraper para actualizar si ya existe.
-- Aquí, para la homogeneización, agrupamos por los campos clave (reporter, address, type)
-- y tomamos el más reciente para la "entidad" del evento, ignorando el timestamp del event_id del scraper.
-- Si prefieres que cada `event_id` del scraper sea tratado como un evento distinto, entonces
-- la homogeneización por `event_id` como en tu original está bien, pero no eliminará duplicados
-- si el scraper recolecta el *mismo* evento múltiples veces en el *mismo* barrido.

RAW_EVENTS = LOAD '/user/hadoop/waze_input/waze_events.tsv'
             USING PigStorage('\t')
             AS (event_id:chararray,type:chararray,address:chararray,timestamp_original_relative:chararray,reporter:chararray,scrape_timestamp:chararray);

-- 1. Filtrar y limpiar: Eliminar registros con campos clave nulos o vacíos
FILTERED_EVENTS = FILTER RAW_EVENTS BY
    type IS NOT NULL AND type != '' AND
    address IS NOT NULL AND address != '' AND
    reporter IS NOT NULL AND reporter != ''; -- También asegurar que el reportero no sea nulo/vacío

-- 2. Homogeneizar/Eliminar "duplicados lógicos" (el mismo incidente reportado varias veces en la misma zona/tipo/reportero)
-- Creamos una clave lógica para agrupar incidentes similares, excluyendo el timestamp de scrape del event_id
HOMOG_KEY_EVENTS = FOREACH FILTERED_EVENTS GENERATE
    event_id, type, address, timestamp_original_relative, reporter, scrape_timestamp,
    CONCAT(reporter, CONCAT('_', CONCAT(REPLACE(REPLACE(address, ' ', ''), '[^a-zA-Z0-9]', ''), CONCAT('_', REPLACE(REPLACE(type, ' ', ''), '[^a-zA-Z0-9]', ''))))) AS logical_event_key;

-- Agrupar por la clave lógica y tomar el evento más reciente de cada grupo
DEDUPED_EVENTS_GROUPED = GROUP HOMOG_KEY_EVENTS BY logical_event_key;

HOMOGENIZED_EVENTS = FOREACH DEDUPED_EVENTS_GROUPED {
    -- Ordenar por scrape_timestamp en orden descendente y tomar el primero (el más reciente)
    ORDERED_EVENTS_IN_GROUP = ORDER HOMOG_KEY_EVENTS BY scrape_timestamp DESC;
    LATEST_EVENT_TUPLE = LIMIT ORDERED_EVENTS_IN_GROUP 1;
    -- Generar solo los campos originales + la clave lógica si se necesita después.
    -- Aquí generamos los campos que usaremos para el análisis.
    GENERATE FLATTEN(LATEST_EVENT_TUPLE.(event_id, type, address, timestamp_original_relative, reporter, scrape_timestamp));
};


-- 3. Clasificar incidentes según su tipo (estandarizado) y comuna
-- Mejorar la detección de comunas y tipos con expresiones regulares más flexibles
INCIDENTS_WITH_COMMUNE = FOREACH HOMOGENIZED_EVENTS GENERATE
    event_id,
    type,
    address,
    timestamp_original_relative,
    reporter,
    scrape_timestamp,
    (CASE
        WHEN address MATCHES '(?i).*Las Condes.*' THEN 'Las Condes'
        WHEN address MATCHES '(?i).*Santiago Centro.*' THEN 'Santiago' -- Más específico para Santiago
        WHEN address MATCHES '(?i).*Santiago.*' THEN 'Santiago'
        WHEN address MATCHES '(?i).*Providencia.*' THEN 'Providencia'
        WHEN address MATCHES '(?i).*Ñuñoa.*' THEN 'Ñuñoa'
        WHEN address MATCHES '(?i).*Maipú.*' THEN 'Maipú'
        WHEN address MATCHES '(?i).*Puente Alto.*' THEN 'Puente Alto'
        WHEN address MATCHES '(?i).*Vitacura.*' THEN 'Vitacura'
        WHEN address MATCHES '(?i).*Lo Barnechea.*' THEN 'Lo Barnechea'
        WHEN address MATCHES '(?i).*La Reina.*' THEN 'La Reina'
        WHEN address MATCHES '(?i).*Macul.*' THEN 'Macul'
        WHEN address MATCHES '(?i).*San Joaquín.*' THEN 'San Joaquín'
        WHEN address MATCHES '(?i).*Independencia.*' THEN 'Independencia'
        WHEN address MATCHES '(?i).*Recoleta.*' THEN 'Recoleta'
        WHEN address MATCHES '(?i).*Quilicura.*' THEN 'Quilicura'
        WHEN address MATCHES '(?i).*Huechuraba.*' THEN 'Huechuraba'
        WHEN address MATCHES '(?i).*Conchalí.*' THEN 'Conchalí'
        WHEN address MATCHES '(?i).*Renca.*' THEN 'Renca'
        WHEN address MATCHES '(?i).*Cerro Navia.*' THEN 'Cerro Navia'
        WHEN address MATCHES '(?i).*Lo Prado.*' THEN 'Lo Prado'
        WHEN address MATCHES '(?i).*Estación Central.*' THEN 'Estación Central'
        WHEN address MATCHES '(?i).*Cerrillos.*' THEN 'Cerrillos'
        WHEN address MATCHES '(?i).*Padre Hurtado.*' THEN 'Padre Hurtado'
        WHEN address MATCHES '(?i).*San Bernardo.*' THEN 'San Bernardo'
        WHEN address MATCHES '(?i).*El Bosque.*' THEN 'El Bosque'
        WHEN address MATCHES '(?i).*La Cisterna.*' THEN 'La Cisterna'
        WHEN address MATCHES '(?i).*La Granja.*' THEN 'La Granja'
        WHEN address MATCHES '(?i).*San Miguel.*' THEN 'San Miguel'
        WHEN address MATCHES '(?i).*Pedro Aguirre Cerda.*' THEN 'Pedro Aguirre Cerda'
        WHEN address MATCHES '(?i).*Lo Espejo.*' THEN 'Lo Espejo'
        WHEN address MATCHES '(?i).*San Ramón.*' THEN 'San Ramón'
        WHEN address MATCHES '(?i).*La Pintana.*' THEN 'La Pintana'
        WHEN address MATCHES '(?i).*Peñalolén.*' THEN 'Peñalolén'
        WHEN address MATCHES '(?i).*La Florida.*' THEN 'La Florida'
        WHEN address MATCHES '(?i).*Pirque.*' THEN 'Pirque'
        WHEN address MATCHES '(?i).*San José de Maipo.*' THEN 'San José de Maipo'
        WHEN address MATCHES '(?i).*Melipilla.*' THEN 'Melipilla'
        WHEN address MATCHES '(?i).*San Pedro.*' THEN 'San Pedro'
        WHEN address MATCHES '(?i).*Curacaví.*' THEN 'Curacaví'
        WHEN address MATCHES '(?i).*María Pinto.*' THEN 'María Pinto'
        WHEN address MATCHES '(?i).*Alhué.*' THEN 'Alhué'
        WHEN address MATCHES '(?i).*Isla de Maipo.*' THEN 'Isla de Maipo'
        WHEN address MATCHES '(?i).*Talagante.*' THEN 'Talagante'
        WHEN address MATCHES '(?i).*El Monte.*' THEN 'El Monte'
        WHEN address MATCHES '(?i).*Peñaflor.*' THEN 'Peñaflor'
        WHEN address MATCHES '(?i).*Calera de Tango.*' THEN 'Calera de Tango'
        WHEN address MATCHES '(?i).*Buin.*' THEN 'Buin'
        WHEN address MATCHES '(?i).*Paine.*' THEN 'Paine'
        WHEN address MATCHES '(?i).*Colina.*' THEN 'Colina'
        WHEN address MATCHES '(?i).*Lampa.*' THEN 'Lampa'
        WHEN address MATCHES '(?i).*Tiltil.*' THEN 'Tiltil'
        ELSE 'Desconocida'
    END) AS commune,
    (CASE
        WHEN type MATCHES '(?i).*(accidente|choque|colisión|volcamiento|crash).*' THEN 'Accidente'
        WHEN type MATCHES '(?i).*(taco|congestión|atasco|lento|retención|traffic|jam).*' THEN 'Atasco'
        WHEN type MATCHES '(?i).*(corte|cerrada|bloqueada|desvío|cierre|road closure).*' THEN 'Corte de Ruta'
        WHEN type MATCHES '(?i).*(peligro|objeto|animal|policía|carabinero|manifestación|evento|cierre|hazard|police|demonstration).*' THEN 'Peligro en Vía'
        WHEN type MATCHES '(?i).*(construcción|works|obra).*' THEN 'Construcción'
        WHEN type MATCHES '(?i).*(calle inundada|inundación|flooded).*' THEN 'Inundación'
        ELSE 'Otro'
    END) AS standardized_type;

-- Almacenar los eventos filtrados y homogeneizados en HDFS (para el siguiente script Pig)
STORE INCIDENTS_WITH_COMMUNE INTO '/user/hadoop/waze_processed/filtered_homogenized_events'
      USING PigStorage('\t');
