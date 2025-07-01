#!/usr/bin/env python3
"""
Dashboard de monitoreo en tiempo real para el video
Muestra métricas del sistema, caché y pipeline
"""

import time
import json
import subprocess
import requests
from datetime import datetime

def clear_screen():
    print("\033[H\033[J", end="")

def get_redis_stats():
    """Obtiene estadísticas del caché Redis."""
    try:
        result = subprocess.run([
            'docker', 'exec', 'waze_cache', 'redis-cli', 'get', 'events:stats'
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout.strip())
    except:
        pass
    return None

def get_elasticsearch_count():
    """Obtiene conteo de documentos en Elasticsearch."""
    try:
        response = requests.get('http://localhost:9200/waze-individual-events/_count', timeout=5)
        if response.status_code == 200:
            return response.json().get('count', 0)
    except:
        pass
    return 0

def get_mongodb_count():
    """Obtiene conteo de eventos en MongoDB."""
    try:
        result = subprocess.run([
            'docker', 'exec', 'waze_storage_db', 'mongosh', '--quiet', '--eval',
            'db.getSiblingDB("waze_db").events.countDocuments()'
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0:
            return int(result.stdout.strip())
    except:
        pass
    return 0

def get_traffic_metrics():
    """Obtiene métricas del generador de tráfico."""
    try:
        result = subprocess.run([
            'docker', 'exec', 'waze_cache', 'redis-cli', 'get', 'traffic_generator:metrics'
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout.strip())
    except:
        pass
    return None

def format_number(num):
    """Formatea números para mejor legibilidad."""
    if num >= 1000000:
        return f"{num/1000000:.1f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    else:
        return str(num)

def main():
    print("🚀 Dashboard de Monitoreo del Sistema de Tráfico Waze")
    print("Presiona Ctrl+C para salir...")
    
    try:
        while True:
            clear_screen()
            
            # Header
            print("=" * 80)
            print("🚦 SISTEMA DE ANÁLISIS DE TRÁFICO WAZE - DASHBOARD EN TIEMPO REAL")
            print("=" * 80)
            print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Datos del pipeline
            mongo_count = get_mongodb_count()
            es_count = get_elasticsearch_count()
            redis_stats = get_redis_stats()
            traffic_metrics = get_traffic_metrics()
            
            # Sección de datos
            print("📊 DATOS DEL PIPELINE:")
            print(f"  📄 MongoDB (datos crudos):      {format_number(mongo_count):>8}")
            print(f"  🔍 Elasticsearch (procesados):  {format_number(es_count):>8}")
            
            if redis_stats:
                print(f"  ⚡ Redis (eventos cacheados):   {format_number(redis_stats.get('total_events', 0)):>8}")
                print(f"  🏘️  Sectores principales:        {len(redis_stats.get('sectores_principales', [])):>8}")
                print(f"  🚨 Tipos de evento:             {len(redis_stats.get('tipos_evento', [])):>8}")
            else:
                print("  ⚡ Redis: No disponible")
            
            print()
            
            # Sección de caché
            print("🔥 RENDIMIENTO DEL CACHÉ:")
            if redis_stats:
                cache_time = redis_stats.get('processing_time_seconds', 0)
                events_per_sec = redis_stats.get('events_per_second', 0)
                print(f"  ⏱️  Tiempo de carga:            {cache_time:>8.2f}s")
                print(f"  🚀 Eventos por segundo:        {events_per_sec:>8.1f}")
                print(f"  🔄 Operaciones realizadas:     {redis_stats.get('cache_operations', 0):>8}")
                
                # Timestamp de última actualización
                cache_updated = redis_stats.get('cache_updated', '')
                if cache_updated:
                    try:
                        update_time = datetime.fromisoformat(cache_updated.replace('Z', '+00:00'))
                        print(f"  📅 Última actualización:       {update_time.strftime('%H:%M:%S')}")
                    except:
                        pass
            else:
                print("  ❌ Estadísticas no disponibles")
            
            print()
            
            # Sección de tráfico simulado
            print("🎯 GENERADOR DE TRÁFICO:")
            if traffic_metrics:
                hit_rate = traffic_metrics.get('hit_rate', 0)
                avg_latency = traffic_metrics.get('average_latency_ms', 0)
                total_queries = traffic_metrics.get('total_queries', 0)
                
                print(f"  📈 Hit Rate:                   {hit_rate:>8.1f}%")
                print(f"  ⚡ Latencia promedio:          {avg_latency:>8.1f}ms")
                print(f"  🔄 Total consultas:            {format_number(total_queries):>8}")
                
                # Indicador visual del hit rate
                hit_indicator = "🟢" if hit_rate >= 80 else "🟡" if hit_rate >= 60 else "🔴"
                latency_indicator = "🟢" if avg_latency <= 50 else "🟡" if avg_latency <= 100 else "🔴"
                
                print(f"  📊 Estado del caché:           {hit_indicator} {latency_indicator}")
            else:
                print("  💤 Generador no activo")
            
            print()
            
            # Enlaces útiles
            print("🌐 ACCESOS RÁPIDOS:")
            print("  📊 Kibana:           http://localhost:5601")
            print("  🔍 Elasticsearch:    http://localhost:9200")
            print("  📄 MongoDB:          mongodb://localhost:27017")
            
            print()
            print("⏳ Actualizando en 5 segundos... (Ctrl+C para salir)")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n👋 Dashboard cerrado. ¡Gracias!")

if __name__ == "__main__":
    main()
