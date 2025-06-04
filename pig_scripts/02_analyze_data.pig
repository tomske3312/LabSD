-- Cargar las JARs del conector MongoDB Hadoop
REGISTER /opt/pig-0.17.0/lib/mongo-hadoop-core-2.0.2.jar;
REGISTER /opt/pig-0.17.0/lib/mongo-hadoop-pig-2.0.2.jar;
REGISTER /opt/pig-0.17.0/lib/mongo-java-driver-3.12.11.jar;
REGISTER /opt/pig-0.17.0/lib/json-simple-1.1.1.jar; -- Necesario para JsonStorage en algunas configuraciones

-- Ruta de entrada: Output del script de filtrado y homogeneización (TSV)
-- La ruta debe ser el directorio que contiene los archivos part-xxxx del STORE anterior
FILTERED_EVENTS = LOAD '/user/hadoop/waze_processed/filtered_homogenized_events'
                 USING PigStorage('\t') AS (
                     event_id:chararray,
                     type:chararray,
                     address:chararray,
                     timestamp_original_relative:chararray,
                     reporter:chararray,
                     scrape_timestamp:chararray,
                     commune:chararray,
                     standardized_type:chararray
                 );

-- Parsear la fecha para análisis temporal (asumiendo formato ISO 8601 de scrape_timestamp)
-- Ejemplo de scrape_timestamp: "2025-06-03T19:44:48.093"
-- Extraer fecha (YYYY-MM-DD) y hora (HH)
EVENTS_WITH_TIME_PARTS = FOREACH FILTERED_EVENTS GENERATE
    event_id,
    type,
    address,
    timestamp_original_relative,
    reporter,
    scrape_timestamp,
    commune,
    standardized_type,
    SUBSTRING(scrape_timestamp, 0, 10) AS event_date,    -- "YYYY-MM-DD"
    SUBSTRING(scrape_timestamp, 11, 13) AS event_hour;  -- "HH" (e.g., "19")

-- 1. Agrupar incidentes por comuna y contar
COMMUNE_GROUPED = GROUP EVENTS_WITH_TIME_PARTS BY commune;
COMMUNE_COUNTS = FOREACH COMMUNE_GROUPED GENERATE
    group AS commune,
    COUNT(EVENTS_WITH_TIME_PARTS) AS total_incidents;

-- Almacenar resultados de comuna en HDFS como JSON
-- Usamos JsonStorage() para que load_pig_results_to_redis.py pueda leerlo fácilmente.
STORE COMMUNE_COUNTS INTO '/user/hadoop/waze_analysis/commune_summary.json'
      USING JsonStorage();

-- 2. Contar la frecuencia de ocurrencia de los diferentes tipos de incidentes
TYPE_GROUPED = GROUP EVENTS_WITH_TIME_PARTS BY standardized_type;
TYPE_COUNTS = FOREACH TYPE_GROUPED GENERATE
    group AS standardized_type,
    COUNT(EVENTS_WITH_TIME_PARTS) AS total_occurrences;

-- Almacenar resultados de tipo en HDFS como JSON
STORE TYPE_COUNTS INTO '/user/hadoop/waze_analysis/type_summary.json'
      USING JsonStorage();

-- 3. Analizar la evolución temporal (ej. por día y tipo/comuna)
-- Resumen diario: incidentes por fecha, tipo y comuna
DAILY_GROUPED = GROUP EVENTS_WITH_TIME_PARTS BY (event_date, standardized_type, commune);
DAILY_SUMMARY = FOREACH DAILY_GROUPED GENERATE
    FLATTEN(group) AS (event_date, standardized_type, commune),
    COUNT(EVENTS_WITH_TIME_PARTS) AS incidents_count;

-- Almacenar resultados diarios en HDFS como JSON
STORE DAILY_SUMMARY INTO '/user/hadoop/waze_analysis/daily_summary.json'
      USING JsonStorage();

-- Resumen horario: incidentes por hora, tipo y comuna
HOURLY_GROUPED = GROUP EVENTS_WITH_TIME_PARTS BY (event_hour, standardized_type, commune);
HOURLY_SUMMARY = FOREACH HOURLY_GROUPED GENERATE
    FLATTEN(group) AS (event_hour, standardized_type, commune),
    COUNT(EVENTS_WITH_TIME_PARTS) AS incidents_count;

-- Almacenar resultados horarios en HDFS como JSON
STORE HOURLY_SUMMARY INTO '/user/hadoop/waze_analysis/hourly_summary.json'
      USING JsonStorage();
