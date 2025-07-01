/* 02_analyze_data.pig - DESHABILITADO: Solo procesamos eventos individuales */

-- Este script se mantiene para futuras agregaciones si son necesarias
-- Por ahora, el pipeline principal trabaja con eventos individuales enriquecidos

-- El script principal ya enriquece los datos individuales en 01_filter_homogenize.pig
-- Si necesitas summaries en el futuro, puedes re-activar este código

/*
REGISTER /opt/pig-0.17.0/lib/piggybank.jar;

FILTERED_EVENTS = LOAD '/user/hadoop/waze_processed/individual_events' USING PigStorage('\t') AS (event_id:chararray, type:chararray, address:chararray, city:chararray, scrape_ts:chararray, commune:chararray, standardized_type:chararray);

EVENTS_WITH_TIME_PARTS = FOREACH FILTERED_EVENTS GENERATE
    commune, standardized_type,
    SUBSTRING(scrape_ts, 0, 10) AS event_date,
    SUBSTRING(scrape_ts, 11, 13) AS event_hour;

COMMUNE_COUNTS = FOREACH (GROUP EVENTS_WITH_TIME_PARTS BY commune) GENERATE group AS commune, COUNT($1) AS total_incidents;
rmf /user/hadoop/waze_analysis/commune_summary.json;
STORE COMMUNE_COUNTS INTO '/user/hadoop/waze_analysis/commune_summary.json' USING JsonStorage();

TYPE_COUNTS = FOREACH (GROUP EVENTS_WITH_TIME_PARTS BY standardized_type) GENERATE group AS standardized_type, COUNT($1) AS total_occurrences;
rmf /user/hadoop/waze_analysis/type_summary.json;
STORE TYPE_COUNTS INTO '/user/hadoop/waze_analysis/type_summary.json' USING JsonStorage();

DAILY_SUMMARY = FOREACH (GROUP EVENTS_WITH_TIME_PARTS BY (event_date, standardized_type, commune)) GENERATE FLATTEN(group), COUNT($1) AS incidents_count;
rmf /user/hadoop/waze_analysis/daily_summary.json;
STORE DAILY_SUMMARY INTO '/user/hadoop/waze_analysis/daily_summary.json' USING JsonStorage();

HOURLY_SUMMARY = FOREACH (GROUP EVENTS_WITH_TIME_PARTS BY (event_hour, standardized_type, commune)) GENERATE FLATTEN(group), COUNT($1) AS incidents_count;
rmf /user/hadoop/waze_analysis/hourly_summary.json;
STORE HOURLY_SUMMARY INTO '/user/hadoop/waze_analysis/hourly_summary.json' USING JsonStorage();
*/