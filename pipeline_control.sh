#!/bin/bash
set -e

# ==============================================================================
# Script de Control Completo del Pipeline de Análisis de Tráfico Waze
# ==============================================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
Pipeline de Análisis de Tráfico Waze - Sistema Distribuido

COMANDOS DISPONIBLES:
  start             Inicia todo el pipeline completo
  stop              Detiene todos los servicios (conserva datos)
  restart           Reinicia todo el pipeline
  status            Muestra el estado de todos los servicios
  clean             Elimina todos los contenedores y volúmenes (¡CUIDADO!)
  logs [servicio]   Muestra logs de un servicio específico o todos
  build             Reconstruye todas las imágenes
  test              Ejecuta tests de conectividad y funcionalidad

SERVICIOS INDIVIDUALES:
  start-scraper     Inicia solo el scraper de Waze
  start-storage     Inicia MongoDB
  start-hadoop      Inicia el cluster Hadoop
  start-analytics   Inicia Elasticsearch y Kibana
  start-cache       Inicia Redis

ACCESOS WEB:
  - Kibana:           http://localhost:5601
  - Elasticsearch:    http://localhost:9200
  - Hadoop NameNode:  http://localhost:9870

EJEMPLOS:
  $0 start          # Inicia todo el pipeline
  $0 logs scraper   # Ve logs del scraper
  $0 status         # Estado de servicios
  $0 stop           # Para servicios sin perder datos

EOF
}

# Verificar que Docker y Docker Compose estén disponibles
check_requirements() {
    log_info "Verificando requisitos del sistema..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado o no está en el PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose no está instalado"
        exit 1
    fi
    
    log_info "✓ Docker y Docker Compose están disponibles"
}

# Función para esperar que un servicio esté saludable
wait_for_service() {
    local service_name=$1
    local max_wait=${2:-300}  # 5 minutos por defecto
    local waited=0
    
    log_info "Esperando que $service_name esté saludable..."
    
    while [ $waited -lt $max_wait ]; do
        if docker-compose ps "$service_name" | grep -q "healthy\|running"; then
            log_info "✓ $service_name está listo"
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
        echo -n "."
    done
    
    log_error "Timeout esperando $service_name después de ${max_wait}s"
    return 1
}

# Mostrar estado de los servicios
show_status() {
    log_info "Estado de los servicios del pipeline:"
    echo ""
    docker-compose ps
    echo ""
    
    # Verificar conectividad de servicios principales
    log_info "Verificando conectividad de servicios..."
    
    # MongoDB
    if docker-compose exec -T storage_db mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; then
        log_info "✓ MongoDB está respondiendo"
    else
        log_warn "✗ MongoDB no está respondiendo"
    fi
    
    # Redis
    if docker-compose exec -T cache redis-cli ping 2>/dev/null | grep -q PONG; then
        log_info "✓ Redis está respondiendo"
    else
        log_warn "✗ Redis no está respondiendo"
    fi
    
    # Elasticsearch
    if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        log_info "✓ Elasticsearch está respondiendo"
    else
        log_warn "✗ Elasticsearch no está respondiendo"
    fi
    
    # Kibana
    if curl -s http://localhost:5601/api/status > /dev/null 2>&1; then
        log_info "✓ Kibana está respondiendo"
    else
        log_warn "✗ Kibana no está respondiendo"
    fi
}

# Iniciar servicios de infraestructura básica
start_infrastructure() {
    log_info "Iniciando servicios de infraestructura..."
    
    docker-compose up -d storage_db cache hadoop-namenode hadoop-datanode
    wait_for_service storage_db 120
    wait_for_service cache 60
    wait_for_service hadoop-namenode 180
    
    docker-compose up -d hadoop-resourcemanager hadoop-nodemanager
    wait_for_service hadoop-nodemanager 120
}

# Iniciar servicios de análisis y visualización
start_analytics() {
    log_info "Iniciando servicios de análisis y visualización..."
    
    docker-compose up -d elasticsearch
    wait_for_service elasticsearch 180
    
    docker-compose up -d kibana
    wait_for_service kibana 120
}

# Iniciar pipeline completo
start_full_pipeline() {
    check_requirements
    
    log_info "=== INICIANDO PIPELINE COMPLETO DE ANÁLISIS DE TRÁFICO ==="
    
    # 1. Servicios de infraestructura
    start_infrastructure
    
    # 2. Servicios de análisis
    start_analytics
    
    # 3. Scraper y procesamiento de datos
    log_info "Iniciando scraper y servicios de procesamiento..."
    docker-compose up -d scraper
    
    # 4. Servicios de procesamiento distribuido
    log_info "Iniciando procesamiento distribuido con Pig..."
    docker-compose up -d pig-runner
    
    # 5. Generador de tráfico (opcional)
    log_info "Iniciando generador de tráfico..."
    docker-compose up -d traffic_generator
    
    log_info "=== PIPELINE INICIADO COMPLETAMENTE ==="
    echo ""
    log_info "Accede a los siguientes servicios:"
    log_info "  📊 Kibana (Visualización): http://localhost:5601"
    log_info "  🔍 Elasticsearch: http://localhost:9200"
    log_info "  🐘 Hadoop NameNode: http://localhost:9870"
    log_info "  📦 MongoDB: localhost:27017"
    log_info "  🗃️  Redis: localhost:6379"
    echo ""
    log_info "Usa '$0 status' para verificar el estado de todos los servicios"
    log_info "Usa '$0 logs [servicio]' para ver logs específicos"
}

# Ejecutar tests de funcionalidad
run_tests() {
    log_info "=== EJECUTANDO TESTS DE FUNCIONALIDAD ==="
    
    # Test 1: Verificar que MongoDB tiene datos
    log_info "Test 1: Verificando datos en MongoDB..."
    event_count=$(docker-compose exec -T storage_db mongosh waze_data --eval "db.events.countDocuments({})" --quiet 2>/dev/null || echo "0")
    if [ "$event_count" -gt 0 ]; then
        log_info "✓ MongoDB contiene $event_count eventos"
    else
        log_warn "✗ MongoDB no contiene eventos aún"
    fi
    
    # Test 2: Verificar que Redis tiene datos de análisis
    log_info "Test 2: Verificando cache en Redis..."
    redis_keys=$(docker-compose exec -T cache redis-cli keys "stats:*" 2>/dev/null | wc -l || echo "0")
    if [ "$redis_keys" -gt 0 ]; then
        log_info "✓ Redis contiene $redis_keys claves de estadísticas"
    else
        log_warn "✗ Redis no contiene estadísticas aún"
    fi
    
    # Test 3: Verificar que Elasticsearch tiene datos
    log_info "Test 3: Verificando datos en Elasticsearch..."
    es_count=$(curl -s "http://localhost:9200/waze-events/_count" 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")
    if [ "$es_count" -gt 0 ]; then
        log_info "✓ Elasticsearch contiene $es_count documentos"
    else
        log_warn "✗ Elasticsearch no contiene documentos aún"
    fi
    
    log_info "=== TESTS COMPLETADOS ==="
}

# Función principal
main() {
    case "${1:-help}" in
        start)
            start_full_pipeline
            ;;
        stop)
            log_info "Deteniendo todos los servicios..."
            docker-compose down
            log_info "✓ Servicios detenidos (datos conservados)"
            ;;
        restart)
            log_info "Reiniciando pipeline..."
            docker-compose down
            start_full_pipeline
            ;;
        status)
            show_status
            ;;
        clean)
            log_warn "¡ATENCIÓN! Esto eliminará TODOS los datos del pipeline."
            read -p "¿Estás seguro? (escribe 'YES' para confirmar): " confirm
            if [ "$confirm" = "YES" ]; then
                docker-compose down -v --remove-orphans
                docker system prune -f
                log_info "✓ Sistema limpiado completamente"
            else
                log_info "Operación cancelada"
            fi
            ;;
        logs)
            if [ -n "$2" ]; then
                docker-compose logs -f "$2"
            else
                docker-compose logs -f
            fi
            ;;
        build)
            log_info "Reconstruyendo todas las imágenes..."
            docker-compose build --no-cache
            log_info "✓ Imágenes reconstruidas"
            ;;
        test)
            run_tests
            ;;
        start-scraper)
            docker-compose up -d scraper
            ;;
        start-storage)
            docker-compose up -d storage_db
            ;;
        start-hadoop)
            docker-compose up -d hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager
            ;;
        start-analytics)
            start_analytics
            ;;
        start-cache)
            docker-compose up -d cache
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Comando no reconocido: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Ejecutar función principal con todos los argumentos
main "$@"
