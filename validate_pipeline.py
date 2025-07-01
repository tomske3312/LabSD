#!/usr/bin/env python3
"""
Script de validación completa del pipeline de análisis de tráfico.
Verifica que todos los componentes funcionen correctamente y el flujo de datos sea correcto.
"""

import os
import sys
import time
import json
import subprocess
import requests
from datetime import datetime

# Colores para output
class Colors:
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color

def log_info(msg):
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")

def log_warn(msg):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}")

def log_error(msg):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")

def log_test(msg):
    print(f"{Colors.BLUE}[TEST]{Colors.NC} {msg}")

def run_command(cmd, timeout=30):
    """Ejecutar comando con timeout"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout"

def check_service_running(service_name):
    """Verificar si un servicio de docker-compose está corriendo"""
    success, output, _ = run_command(f"docker-compose ps {service_name}")
    if success and "Up" in output:
        return True
    return False

def test_mongodb_connection():
    """Test de conexión y datos en MongoDB"""
    log_test("Verificando MongoDB...")
    
    if not check_service_running("storage_db"):
        log_error("MongoDB no está corriendo")
        return False
    
    # Test de conexión
    success, output, error = run_command(
        "docker-compose exec -T storage_db mongosh --eval 'db.adminCommand(\"ping\")' --quiet"
    )
    
    if not success:
        log_error(f"No se puede conectar a MongoDB: {error}")
        return False
    
    # Verificar datos
    success, output, _ = run_command(
        "docker-compose exec -T storage_db mongosh waze_data --eval 'db.events.countDocuments({})' --quiet"
    )
    
    if success:
        try:
            count = int(output.strip())
            if count > 0:
                log_info(f"✓ MongoDB tiene {count} eventos")
                return True
            else:
                log_warn("MongoDB no tiene eventos aún")
                return False
        except ValueError:
            log_warn("No se pudo obtener el count de MongoDB")
            return False
    
    return False

def test_redis_connection():
    """Test de conexión y datos en Redis"""
    log_test("Verificando Redis...")
    
    if not check_service_running("cache"):
        log_error("Redis no está corriendo")
        return False
    
    # Test de conexión
    success, output, _ = run_command("docker-compose exec -T cache redis-cli ping")
    
    if not success or "PONG" not in output:
        log_error("No se puede conectar a Redis")
        return False
    
    # Verificar datos de estadísticas
    success, output, _ = run_command("docker-compose exec -T cache redis-cli keys 'stats:*'")
    
    if success:
        keys = [key for key in output.strip().split('\n') if key.strip()]
        if keys:
            log_info(f"✓ Redis tiene {len(keys)} claves de estadísticas")
            return True
        else:
            log_warn("Redis no tiene estadísticas aún")
            return False
    
    return False

def test_elasticsearch():
    """Test de Elasticsearch"""
    log_test("Verificando Elasticsearch...")
    
    if not check_service_running("elasticsearch"):
        log_error("Elasticsearch no está corriendo")
        return False
    
    try:
        # Test de conexión
        response = requests.get("http://localhost:9200/_cluster/health", timeout=10)
        if response.status_code != 200:
            log_error("Elasticsearch no responde correctamente")
            return False
        
        health = response.json()
        log_info(f"✓ Elasticsearch estado: {health.get('status', 'unknown')}")
        
        # Verificar índice de datos
        response = requests.get("http://localhost:9200/waze-events/_count", timeout=10)
        if response.status_code == 200:
            count_data = response.json()
            count = count_data.get('count', 0)
            if count > 0:
                log_info(f"✓ Elasticsearch tiene {count} documentos indexados")
                return True
            else:
                log_warn("Elasticsearch no tiene documentos indexados aún")
                return False
        else:
            log_warn("Índice waze-events no existe aún")
            return False
            
    except requests.exceptions.RequestException as e:
        log_error(f"Error conectando a Elasticsearch: {e}")
        return False

def test_kibana():
    """Test de Kibana"""
    log_test("Verificando Kibana...")
    
    if not check_service_running("kibana"):
        log_error("Kibana no está corriendo")
        return False
    
    try:
        response = requests.get("http://localhost:5601/api/status", timeout=15)
        if response.status_code == 200:
            log_info("✓ Kibana está respondiendo")
            return True
        else:
            log_error(f"Kibana responde con código {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        log_error(f"Error conectando a Kibana: {e}")
        return False

def test_hadoop():
    """Test de Hadoop"""
    log_test("Verificando Hadoop...")
    
    if not check_service_running("hadoop-namenode"):
        log_error("Hadoop NameNode no está corriendo")
        return False
    
    try:
        response = requests.get("http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus", timeout=10)
        if response.status_code == 200:
            log_info("✓ Hadoop NameNode está respondiendo")
            return True
        else:
            log_error("Hadoop NameNode no responde correctamente")
            return False
    except requests.exceptions.RequestException as e:
        log_error(f"Error conectando a Hadoop: {e}")
        return False

def test_scraper_data():
    """Test de datos del scraper"""
    log_test("Verificando datos del scraper...")
    
    if not check_service_running("scraper"):
        log_error("Scraper no está corriendo")
        return False
    
    # Verificar que existe el archivo de datos
    success, output, _ = run_command(
        "docker-compose exec -T scraper test -f /app/data/waze_events.json"
    )
    
    if not success:
        log_warn("Archivo de datos del scraper no existe aún")
        return False
    
    # Verificar contenido del archivo
    success, output, _ = run_command(
        "docker-compose exec -T scraper wc -l /app/data/waze_events.json"
    )
    
    if success:
        try:
            lines = int(output.strip().split()[0])
            if lines > 0:
                log_info(f"✓ Archivo del scraper tiene {lines} líneas")
                return True
            else:
                log_warn("Archivo del scraper está vacío")
                return False
        except (ValueError, IndexError):
            log_warn("No se pudo verificar el contenido del archivo del scraper")
            return False
    
    return False

def test_data_flow():
    """Test del flujo completo de datos"""
    log_test("Verificando flujo completo de datos...")
    
    tests = [
        ("Scraper → Datos", test_scraper_data),
        ("MongoDB", test_mongodb_connection),
        ("Redis Cache", test_redis_connection),
        ("Elasticsearch", test_elasticsearch),
        ("Kibana", test_kibana),
        ("Hadoop", test_hadoop)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            if result:
                log_info(f"✓ {test_name} - PASS")
            else:
                log_warn(f"✗ {test_name} - FAIL")
        except Exception as e:
            log_error(f"✗ {test_name} - ERROR: {e}")
            results.append((test_name, False))
    
    return results

def show_service_status():
    """Mostrar estado de todos los servicios"""
    log_info("Estado de servicios:")
    
    services = [
        "scraper", "importer", "storage_db", "cache",
        "hadoop-namenode", "hadoop-datanode", 
        "hadoop-resourcemanager", "hadoop-nodemanager",
        "pig-runner", "elasticsearch", "kibana",
        "elasticsearch_importer", "traffic_generator"
    ]
    
    for service in services:
        if check_service_running(service):
            print(f"  ✓ {service}")
        else:
            print(f"  ✗ {service}")

def main():
    """Función principal de validación"""
    print("=" * 60)
    print("🔍 VALIDACIÓN COMPLETA DEL PIPELINE DE ANÁLISIS DE TRÁFICO")
    print("=" * 60)
    print()
    
    # Mostrar estado de servicios
    show_service_status()
    print()
    
    # Ejecutar tests del flujo de datos
    print("🧪 Ejecutando tests de flujo de datos...")
    print()
    
    results = test_data_flow()
    
    # Resumen de resultados
    print()
    print("=" * 60)
    print("📊 RESUMEN DE RESULTADOS")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} {test_name}")
    
    print()
    print(f"📈 Score: {passed}/{total} tests pasaron ({passed/total*100:.1f}%)")
    
    if passed == total:
        log_info("🎉 ¡TODOS LOS TESTS PASARON! El pipeline está funcionando correctamente.")
    elif passed >= total * 0.8:
        log_warn("⚠️ La mayoría de tests pasaron. Algunos servicios pueden estar iniciándose aún.")
    else:
        log_error("❌ Múltiples tests fallaron. Revisa los logs de los servicios.")
    
    print()
    print("🔗 ACCESOS RÁPIDOS:")
    print("  📊 Kibana:           http://localhost:5601")
    print("  🔍 Elasticsearch:    http://localhost:9200")
    print("  🐘 Hadoop NameNode:  http://localhost:9870")
    print()
    print("💡 Para logs detallados: ./pipeline_control.sh logs [servicio]")
    
    return passed == total

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log_info("Validación cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        log_error(f"Error inesperado: {e}")
        sys.exit(1)
