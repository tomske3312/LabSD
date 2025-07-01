#!/bin/bash

# Script de verificación rápida del sistema
echo "🔍 VERIFICACIÓN RÁPIDA DEL SISTEMA"
echo "=================================="

# 1. Verificar servicios activos
echo "📦 Servicios Docker activos:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(waze|hadoop|elastic|kibana|cache)"

echo ""
echo "💾 Datos en MongoDB:"
docker exec -it waze_storage_db mongosh --quiet --eval "
try { 
  db = db.getSiblingDB('waze_db'); 
  print('Eventos en MongoDB: ' + db.events.countDocuments()); 
} catch(e) { 
  print('Error: ' + e); 
}"

echo ""
echo "🗃️ Datos en HDFS:"
docker exec hadoop-namenode hdfs dfs -ls /user/hadoop/waze_processed/ 2>/dev/null || echo "HDFS no disponible"

echo ""
echo "🔍 Datos en Elasticsearch:"
curl -s "http://localhost:9200/waze-individual-events/_count" | python3 -m json.tool 2>/dev/null || echo "Elasticsearch no disponible"

echo ""
echo "⚡ Caché Redis:"
docker exec waze_cache redis-cli eval "
local keys = redis.call('keys', 'events:*')
local stats = redis.call('get', 'events:stats')
redis.call('set', 'temp_output', 'Claves en caché: ' .. #keys)
if stats then
  local data = cjson.decode(stats)
  redis.call('append', 'temp_output', '\nEventos cacheados: ' .. (data.total_events or 0))
end
return redis.call('get', 'temp_output')
" 0 2>/dev/null || echo "Redis no disponible"

echo ""
echo "🌐 Servicios web:"
echo "- Kibana: http://localhost:5601"
echo "- Elasticsearch: http://localhost:9200"
echo ""
