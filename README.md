# Taller 3: Spark & Arquitectura Medallion

Este taller práctico forma parte del **Módulo 3: Spark** y se centra en la implementación del **Capítulo 11: Arquitectura Medallion**.
El objetivo es simular un flujo de datos real (Lakehouse) y resolver un reto de ingeniería de datos: la implementación de una **Puerta de Calidad (Quality Gate)**.

## 🎯 Objetivos del Taller

1. **Infraestructura:** Desplegar un clúster Spark completo (Master, Worker, Jupyter) usando Docker.
2. **Arquitectura:** Construir un pipeline de datos con capas **Bronce** (Crudo), **Plata** (Limpio) y **Oro** (Agregado).
3. **RETO (Quality Gate):** Modificar el flujo para detectar datos corruptos y desviarlos a una zona de **Cuarentena** en lugar de eliminarlos.

## 🛠️ Prerrequisitos

* Docker Desktop instalado y corriendo.
* Git (opcional, para clonar el repo).

---

## 🚀 Guía Paso a Paso

### 1. Despliegue del Cluster

Levanta los servicios definidos en el `docker-compose.yml`:

```bash
docker-compose up --build -d

```

Verifica que los contenedores estén activos:

* **Jupyter Lab (Tu entorno de trabajo):** [http://localhost:8888](https://www.google.com/search?q=http://localhost:8888)
* **Spark Master UI (Monitorización):** [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)

### 2. Ingesta (Capa Bronce)

El primer paso es convertir los datos crudos a un formato optimizado (Delta Lake).

* Entra a Jupyter y abre `notebooks/01_ingest.py`.
* **Acción:** Ejecuta el script.
* **Resultado esperado:** Se creará la tabla Delta en `data/lakehouse/bronze/secop`.

### 3. 🔥 EL RETO: Transformación y Quality Gate (Capa Plata)

Aquí aplicarás los conceptos del Capítulo 11. El script original `02_transform.py` simplemente borra los datos malos, lo cual es una mala práctica en auditoría.

**Tu Misión:**
Modificar `02_transform.py` para implementar una lógica de bifurcación (split):

1. **Reglas de Calidad:**
* `Precio Base` debe ser mayor a 0.
* `Fecha de Firma` no puede ser nula.


2. **Ruteo de Datos:**
* ✅ **Registros Válidos:** Guardarlos en la tabla `silver/secop`.
* ❌ **Registros Inválidos:** Guardarlos en una nueva tabla `quarantine/secop_errors`, agregando una columna `motivo_rechazo`.



> *Pista: Utiliza funciones como `when().otherwise()` y filtros inversos (`~`) para separar los DataFrames.*

### 4. Analítica de Negocio (Capa Oro)

Con los datos saneados en la capa Plata, generaremos valor para el negocio.

* Abre y ejecuta `notebooks/03_analytics.py`.
* **Resultado:** Se generará una tabla agregada en `data/lakehouse/gold/top_deptos` mostrando la inversión por departamento.

---

## 📂 Estructura del Lakehouse

El taller generará la siguiente estructura de carpetas en `data/lakehouse/`:

| Capa | Ruta | Descripción | Formato |
| --- | --- | --- | --- |
| **Bronce** | `/bronze/secop` | Copia fiel del CSV original (Raw). | Delta |
| **Plata** | `/silver/secop` | Datos limpios, tipados y validados. | Delta |
| **Cuarentena** | `/quarantine/secop_errors` | Datos corruptos para auditoría (Resultado del Reto). | Delta |
| **Oro** | `/gold/top_deptos` | Agregaciones listas para reportes/dashboards. | Delta |

---

## 🧹 Limpieza

Para detener y borrar todo (incluyendo los datos generados):

```bash
docker-compose down
# Opcional: borrar la carpeta data/lakehouse manualmente para reiniciar el taller

```

---

## 🗂️ Entregables y Documentación
Este repositorio incluye los entregables solicitados para el reto:

- `medallion_pipeline.py` — Script completo del pipeline (ingesta, Quality Gate, split a quarantine y reprocessing).
- `quality_report.sql` — Queries para monitoreo y creación de dashboards de calidad.
- `README.md` — Documentación con decisiones técnicas, reglas y ejemplo de ejecución.

### Reglas de Calidad (resumen)
- **Precio Base (`Precio_Base`)**: NOT NULL y > 0 (códigos E003, E004)
- **Fecha de Firma (`Fecha_de_Firma`)**: NOT NULL y no futura (códigos E001, E002)

### Metadatos en Quarantine
Los registros desviados a `quarantine/secop_errors` contienen:
- `motivo_rechazo`, `_rejection_code`, `_validation_timestamp`, `_pipeline_version`, `_quarantine_id`, `_reprocess_attempts`.

### Ejecución (ejemplos)
Ejecutar pipeline con ingesta y transformación:

```bash
docker compose exec jupyter-lab python /app/medallion_pipeline.py \
  --input-path /app/data/SECOP_II_Contratos_Electronicos.csv \
  --lakehouse-root /app/data/lakehouse --pipeline-version v1 --mode dev
```

Reprocesar registros en cuarentena:

```bash
docker compose exec jupyter-lab python /app/medallion_pipeline.py --run-reprocess --lakehouse-root /app/data/lakehouse
```

---

### Siguientes pasos recomendados
1. Añadir pruebas unitarias (`pytest`) para las funciones de validación.
2. Agregar un pipeline CI (GitHub Actions) que ejecute tests en cada PR.
3. Construir un dashboard usando `quality_report.sql` para monitoreo continuo.

---

Si quieres que implemente los tests y la configuración de CI ahora, puedo crear los archivos de prueba y el workflow de GitHub Actions.

