# 🚦 Sistema de Procesamiento de Datos de Tráfico Waze - RM

Sistema distribuido completo para procesar eventos de tráfico de Waze en la Región Metropolitana, con procesamiento Apache Pig, caché Redis inteligente y visualización en tiempo real.

**Autores:** Jose Olave y Sebastián Gulfo  
**Curso:** Sistemas Distribuidos 2025-1  
**Entregas:** 1, 2 y 3 (Sistema completo con visualización)

## 🏗️ Arquitectura del Sistema

```
Waze API → Scraper → MongoDB → Apache Pig → Elasticsearch → Kibana
                                    ↓
                              Redis (Caché LRU)
```

### 📦 Componentes del Sistema:
- **MongoDB**: Almacenamiento de datos crudos de Waze
- **Apache Pig + Hadoop**: Procesamiento distribuido y enriquecimiento
- **Elasticsearch**: Indexación para búsquedas rápidas  
- **Redis**: Caché inteligente para consultas frecuentes
- **Kibana**: Dashboards interactivos y visualización

## 🚀 Inicio Rápido

### Prerrequisitos
- Docker y Docker Compose instalados
- 8GB RAM mínimo
- 10GB espacio libre en disco

### Comandos Principales

#### 🔧 **Iniciar Sistema Completo**
```bash
# Construir e iniciar todos los servicios
docker compose up --build -d

# Verificar que todos los servicios estén corriendo
docker compose ps
```

#### 📊 **Verificar Estado del Sistema**
```bash
# Script de verificación completa
./verify_system.sh

# Ver logs del pipeline de procesamiento
docker compose logs waze_pig_runner

# Verificar datos en Elasticsearch
curl "http://localhost:9200/waze-individual-events/_count"
```

#### 🔍 **Monitoreo y Logs**
```bash
# Ver logs de un servicio específico
docker compose logs -f elasticsearch
docker compose logs -f kibana

# Ver todos los logs del sistema
docker compose logs
```

#### ⚡ **Control de Servicios Específicos**
```bash
# Iniciar servicios específicos (sin rebuild)
docker compose up elasticsearch kibana

# Detener servicios específicos  
docker compose down elasticsearch kibana

# Reiniciar solo el pipeline de procesamiento
docker compose restart waze_pig_runner
```

#### 🧹 **Limpieza y Reset**
```bash
# Detener todos los servicios
docker compose down

# Limpiar volúmenes y datos (RESET COMPLETO)
docker compose down -v
docker system prune -f

# Reiniciar desde cero
docker compose up --build -d
```

## 🌐 Acceso a Servicios

Una vez iniciado el sistema, accede a:

- **📊 Kibana (Visualización)**: http://localhost:5601
- **🔍 Elasticsearch (API)**: http://localhost:9200  
- **💾 MongoDB**: mongodb://localhost:27017
- **⚡ Redis**: redis://localhost:6379

## 📈 Métricas del Sistema

El sistema procesa datos y genera métricas en tiempo real:

```bash
# Ver métricas del caché Redis
docker exec waze_cache redis-cli info stats

# Verificar eventos procesados
docker logs waze_pig_runner | grep "MÉTRICAS DE CACHÉ"

# Estado de todos los servicios
docker compose ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## 🎯 Resultados Esperados

Después del procesamiento completo verás:
- ✅ **2,400+ eventos** procesados desde Waze
- ✅ **240+ sectores** geográficos identificados
- ✅ **4 tipos** de eventos clasificados
- ✅ **88 operaciones** de caché realizadas
- ✅ **Dashboards** interactivos en Kibana

## 🔧 Resolución de Problemas Comunes

### Problema: Servicios no inician
```bash
# Verificar Docker disponible
docker --version

# Liberar puertos si están ocupados
sudo lsof -i :5601  # Kibana
sudo lsof -i :9200  # Elasticsearch

# Reiniciar Docker
sudo systemctl restart docker
```

### Problema: Sin datos en Kibana
```bash
# Verificar pipeline completado
docker logs waze_pig_runner

# Reejecutar procesamiento
docker compose restart waze_pig_runner

# Verificar índice creado
curl "http://localhost:9200/_cat/indices"
```

### Problema: Falta memoria
```bash
# Verificar memoria disponible
free -h

# Configurar Elasticsearch para menos memoria
export ES_JAVA_OPTS="-Xms512m -Xmx1g"
docker compose up elasticsearch
```

## 📋 Comandos de Desarrollo

### Útiles para Debugging:
```bash
# Ejecutar comando dentro de un contenedor
docker exec -it waze_storage_db mongosh

# Ver archivos en HDFS
docker exec hadoop-namenode hdfs dfs -ls /user/hadoop/

# Conectar a Redis CLI
docker exec -it waze_cache redis-cli

# Ver estructura de contenedores
docker compose config
```

### Para Desarrollo y Testing:
```bash
# Verificación rápida del sistema
./verify_system.sh

# Ver estado de todos los servicios
docker compose ps

# Monitoreo en tiempo real
docker compose logs -f
```

## 🐳 Guía Completa de Docker Compose

### Configuración de Servicios

El sistema utiliza Docker Compose para orquestar todos los servicios necesarios:

```yaml
# docker-compose.yml estructura:
# - mongodb (Base de datos principal)
# - elasticsearch (Motor de búsqueda)
# - kibana (Visualización)
# - redis (Caché)
# - hadoop-namenode (HDFS)
# - waze_pig_runner (Pipeline de procesamiento)
```

### Comandos Avanzados

#### 🔄 **Reinicio Completo del Sistema**
```bash
# Detener todo y limpiar volúmenes
docker compose down -v

# Reconstruir imágenes desde cero
docker compose build --no-cache

# Iniciar sistema con logs en tiempo real
docker compose up --build
```

#### 🚀 **Inicio Secuencial (Recomendado)**
```bash
# 1. Iniciar servicios base primero
docker compose up -d mongodb redis elasticsearch

# 2. Esperar a que estén listos (30 segundos)
sleep 30

# 3. Iniciar Hadoop y pipeline
docker compose up -d hadoop-namenode waze_pig_runner

# 4. Finalmente Kibana
docker compose up -d kibana
```

#### 📊 **Verificación de Estado**
```bash
# Estado detallado de contenedores
docker compose ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Health check de servicios críticos
curl -s http://localhost:9200/_cluster/health | jq .status
curl -s http://localhost:5601/api/status | jq .status.overall.state

# Verificar datos en MongoDB
docker exec waze_storage_db mongosh --eval "db.waze_events.countDocuments()"
```

#### 🔍 **Debugging y Monitoreo**
```bash
# Logs en tiempo real de todos los servicios
docker compose logs -f

# Logs de servicios específicos
docker compose logs -f waze_pig_runner elasticsearch

# Ver recursos utilizados
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"

# Inspeccionar red de Docker
docker network inspect labsd_waze_network


### Solución de Problemas Específicos

#### 🚨 **Error de Conexión entre Servicios**

# Verificar red de Docker
docker network ls
docker network inspect labsd_waze_network

# Reiniciar servicios en orden
docker compose restart mongodb
sleep 10
docker compose restart elasticsearch redis
sleep 10
docker compose restart waze_pig_runner


#### 💾 **Problemas de Espacio en Disco**

# Verificar espacio utilizado
df -h
docker system df

# Limpiar datos antiguos
docker system prune -f
docker volume prune -f

# Limpiar solo datos de este proyecto
docker compose down -v


#### ⚡ **Optimización de Performance**
```bash
# Configurar memoria para Elasticsearch
export ES_JAVA_OPTS="-Xms1g -Xmx2g"

# Verificar memoria del sistema
free -h

# Configurar límites de memoria para servicios
docker compose --compatibility up
```

---

**🚀 Sistema Distribuido de Procesamiento Waze - Entrega 3 Completa**

*Desarrollado por Jose Olave y Sebastián Gulfo - Sistemas Distribuidos 2025-1*


