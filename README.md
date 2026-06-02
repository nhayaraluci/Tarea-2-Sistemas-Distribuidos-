# Tarea 2 - Sistemas Distribuidos

## Arquitectura de Microservicios con Kafka, Caché, Retry y Dead Letter Queue

### Integrantes


---

# 1. Descripción General

Este proyecto implementa una arquitectura distribuida basada en microservicios que utilizan Apache Kafka para la comunicación asíncrona.

El sistema procesa consultas sobre un conjunto de datos de edificaciones distribuidas por zonas geográficas. Para mejorar el rendimiento se utiliza una caché LRU y para aumentar la tolerancia a fallos se implementan mecanismos de Retry y Dead Letter Queue (DLQ).

---

# 2. Objetivos de la Tarea

La solución implementa los siguientes conceptos vistos en el curso:

* Arquitectura basada en microservicios.
* Comunicación mediante Apache Kafka.
* Procesamiento asíncrono de consultas.
* Caché distribuida tipo LRU.
* Manejo de errores mediante Retry.
* Manejo de errores permanentes mediante Dead Letter Queue.
* Generación de carga para evaluación del sistema.

---

# 3. Arquitectura General

```text
                    +------------------+
                    | Traffic Generator|
                    +--------+---------+
                             |
                             v
                        Kafka Topic
                          queries
                             |
                             v
                    +----------------+
                    | Consumer Worker|
                    +-------+--------+
                            |
                +-----------+-----------+
                |                       |
            Success                  Failure
                |                       |
                v                       v
         Cache Service           queries.retry
                                         |
                                         v
                               +----------------+
                               | Retry Worker   |
                               +--------+-------+
                                        |
                         +--------------+--------------+
                         |                             |
                     Success                     Failure
                         |                             |
                         v                             v
                   Cache Service             queries.dlq
                                                     |
                                                     v
                                           +----------------+
                                           | DLQ Worker     |
                                           +----------------+

                              |
                              v

                     +----------------------+
                     | Response Generator   |
                     +----------------------+
```

---

# 4. Componentes del Sistema

## 4.1 Traffic Generator

Archivo:

```text
traffic.py
```

Responsabilidades:

* Generar consultas aleatorias.
* Publicar mensajes en Kafka.
* Simular carga de trabajo.
* Permitir experimentos de rendimiento.

Las consultas son enviadas al tópico:

```text
queries
```

---

## 4.2 Consumer

Archivo:

```text
consumer.py
```

Responsabilidades:

* Consumir consultas desde Kafka.
* Enviar solicitudes al Cache Service.
* Registrar métricas de latencia.
* Detectar errores.

Cuando ocurre una falla:

```text
queries
    ↓
consumer
    ↓
queries.retry
```

---

## 4.3 Retry Worker

Archivo:

```text
retry_worker.py
```

Responsabilidades:

* Consumir mensajes desde:

```text
queries.retry
```

* Reintentar consultas fallidas.
* Incrementar contador de intentos.
* Reenviar nuevamente a retry.
* Enviar al DLQ cuando se supera el límite.

Flujo:

```text
queries.retry
      ↓
retry_worker
      ↓
¿éxito?
  ├─ sí → termina
  └─ no → retry o DLQ
```

---

## 4.4 Dead Letter Queue Worker

Archivo:

```text
dlq_queries.py
```

Responsabilidades:

* Consumir mensajes desde:

```text
queries.dlq
```

* Registrar mensajes imposibles de procesar.
* Evitar pérdida de información.
* Permitir análisis posterior.

Ejemplo de mensaje DLQ:

```json
{
    "query_id": "...",
    "query_type": "Q_FAIL_RETRY",
    "_retry_count": 3,
    "_status": "DLQ"
}
```

---

## 4.5 Cache Service

Archivo principal:

```text
main.py
```

Endpoint:

```text
POST /query
```

Responsabilidades:

* Recibir consultas.
* Generar clave única.
* Verificar caché.
* Resolver consultas nuevas.
* Almacenar resultados.

---

# 5. Caché LRU

La caché implementa la política:

```text
Least Recently Used (LRU)
```

Funcionamiento:

* Si una consulta ya fue realizada:

  * Cache Hit.

* Si la consulta no existe:

  * Cache Miss.
  * Se calcula el resultado.
  * Se almacena en caché.

Ejemplo:

```text
[CACHE HIT] ✔
```

```text
[CACHE MISS] ❌
```

Beneficios:

* Menor tiempo de respuesta.
* Menor carga computacional.
* Menor uso del servicio generador.

---

# 6. Response Generator

Responsabilidades:

* Resolver consultas sobre el dataset.
* Calcular resultados solicitados.
* Entregar respuesta al Cache Service.

El Cache Service actúa como orquestador y utiliza este servicio únicamente cuando ocurre un Cache Miss.

---

# 7. Kafka Topics Utilizados

## queries

Tópico principal.

Contiene:

```text
Consultas nuevas
```

---

## queries.retry

Tópico de reintentos.

Contiene:

```text
Consultas que fallaron
```

---

## queries.dlq

Dead Letter Queue.

Contiene:

```text
Consultas imposibles de procesar
```

---

# 8. Experimento Retry

Procedimiento:

1. Apagar temporalmente un servicio.
2. Generar tráfico.
3. Observar errores.
4. Verificar envío a retry.

Resultado esperado:

```text
ACTION : SEND TO RETRY
```

Posteriormente:

```text
RETRY PROCESS
ATTEMPT : 1/3
```

---

# 9. Experimento Dead Letter Queue

Procedimiento:

1. Generar consultas especiales:

```text
Q_FAIL_RETRY
```

2. Forzar fallo permanente.

3. Observar:

```text
ATTEMPT : 1/3
ATTEMPT : 2/3
```

4. Verificar envío a DLQ.

Resultado esperado:

```text
ACTION : SEND TO DLQ
```

Y luego:

```text
DEAD LETTER QUEUE MESSAGE
STATUS : DLQ
```

---

# 10. Cómo Ejecutar

## Construcción

```bash
docker compose build
```

## Levantar servicios

```bash
docker compose up
```

## Ejecutar en segundo plano

```bash
docker compose up -d
```

---

# 11. Ver Logs

Consumer:

```bash
docker logs -f consumer
```

Retry:

```bash
docker logs -f retry-worker
```

DLQ:

```bash
docker logs -f dlq-worker
```

Cache:

```bash
docker logs -f cache-service
```

---

# 12. Conclusiones

La solución implementa exitosamente una arquitectura distribuida basada en Kafka que incorpora:

* Procesamiento asíncrono.
* Caché LRU.
* Retry automático.
* Dead Letter Queue.
* Generación de carga.
* Comunicación desacoplada mediante tópicos.

El sistema es capaz de recuperarse de fallos temporales mediante reintentos y aislar fallos permanentes mediante DLQ, aumentando significativamente su robustez y confiabilidad.
