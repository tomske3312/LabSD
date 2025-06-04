# :computer: :lock: Entregas Tareas - Sistemas Distribuidos

Repositorio con los códigos usados para el desarrollo de la tarea del curso de Sistemas Distribuidos.




**Autor:** Jose Olave y Sebastián Gulfo

**Fecha inicial:** Abril 2025







Comandos utiles utilizados:


Build todos los docker

docker compose up --build -d


Ver todos los docker Encendidos ahora mismo

docker compose ps


LOGS de un docker especifico.

docker compose logs traffic_generator




Prender  prender uno especifico sin rebuildearlo 

docker compose up redis_cache traffic_generator


Apagar uno especificoseleccion

docker compose down redis_cache traffic_generator



Borrar Datos guardados por dockers como volumenes y memorias para ponerlos de 0. especificos para los redis cache y traffic_generator

docker compose down -v redis_cache traffic_generator

Reinicio del generador de trafico y el SIstema de cache seguido de eliminar su memoria interna, REbuildearlos. esperar 1 minuto ES IMPORTANTE QUE ESPERE UN MINUTO PORQUE SI SE PRENDEN TODOS A LA VEZ SIEMPRE HABRA ERROR EN EL GENERADOR DE TRAFICO QUE INTENTARA CONECTARSE A UNA BASE DE DATOS QUE AUN NO ESTA INICIALIZADA ENTONCES ESTOS ULTIMOS HAY QUE REINICIARLOS

docker compose down redis_cache traffic_generator && docker compose down -v redis_cache traffic_generator && docker compose down && docker compose up --build -d && docker compose down redis_cache traffic_generator && sleep 60 docker compose up redis_cache traffic_generator


Todos los resultados

LOG


