-- quality_report.sql
-- Consultas para monitoreo de calidad y cuarentena

-- 1) Resumen diario de rechazos por código
SELECT
  CAST(_validation_timestamp AS DATE) as day,
  COUNT(*) as total_quarantined,
  SUM(CASE WHEN _rejection_code='E001' THEN 1 ELSE 0 END) as e001_fecha_nula,
  SUM(CASE WHEN _rejection_code='E002' THEN 1 ELSE 0 END) as e002_fecha_futura,
  SUM(CASE WHEN _rejection_code='E003' THEN 1 ELSE 0 END) as e003_precio_nulo,
  SUM(CASE WHEN _rejection_code='E004' THEN 1 ELSE 0 END) as e004_precio_no_positivo
FROM quarantine.secop_errors
GROUP BY CAST(_validation_timestamp AS DATE)
ORDER BY day DESC;

-- 2) % rechazo vs procesados por día
WITH q AS (
  SELECT CAST(_validation_timestamp AS DATE) day, count(*) q_count FROM quarantine.secop_errors GROUP BY 1
), s AS (
  SELECT CAST(_ingestion_time AS DATE) day, count(*) s_count FROM silver.secop GROUP BY 1
)
SELECT q.day, q.q_count, COALESCE(s.s_count,0) s_count,
  ROUND(100.0*q.q_count/(q.q_count + COALESCE(s.s_count,0)),2) as reject_pct
FROM q LEFT JOIN s ON q.day = s.day
ORDER BY day DESC;

-- 3) Top motivos de rechazo (últimos 30 días)
SELECT motivo_rechazo, count(*) as cnt
FROM quarantine.secop_errors
WHERE _validation_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY motivo_rechazo
ORDER BY cnt DESC
LIMIT 10;

-- 4) Muestra de registros inválidos recientes
SELECT entidad, Precio_Base, Fecha_de_Firma, motivo_rechazo, _validation_timestamp
FROM quarantine.secop_errors
ORDER BY _validation_timestamp DESC
LIMIT 50;
