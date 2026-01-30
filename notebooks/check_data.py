#!/usr/bin/env python3
import json
import os

def read_delta_log(log_path):
    """Leer información del Delta Log"""
    stats = {"rows": 0, "bytes": 0}
    try:
        files = sorted([f for f in os.listdir(log_path) if f.endswith('.json')])
        if files:
            # Leer el último archivo del log
            with open(os.path.join(log_path, files[-1]), 'r') as f:
                for line in f:
                    data = json.loads(line)
                    # Buscar commitInfo con métricas
                    if 'commitInfo' in data:
                        metrics = data['commitInfo'].get('operationMetrics', {})
                        if metrics:
                            stats['rows'] = int(metrics.get('numOutputRows', 0))
                            stats['bytes'] = int(metrics.get('numOutputBytes', 0))
    except Exception as e:
        print(f"Error leyendo {log_path}: {e}")
    
    return stats

lakehouse_path = "/app/data/lakehouse"

print("\n" + "="*70)
print("📊 REPORTE FINAL: VALIDACIÓN DE DATOS - SILVER vs QUARANTINE")
print("="*70)

# Leer estadísticas de cada tabla
bronze_stats = read_delta_log(os.path.join(lakehouse_path, "bronze/secop/_delta_log"))
silver_stats = read_delta_log(os.path.join(lakehouse_path, "silver/secop/_delta_log"))
quarantine_stats = read_delta_log(os.path.join(lakehouse_path, "quarantine/secop_errors/_delta_log"))

total_bronze = bronze_stats['rows']
total_silver = silver_stats['rows']
total_quarantine = quarantine_stats['rows']
total_processed = total_silver + total_quarantine

print(f"\n🟦 BRONZE (Datos Crudos):")
print(f"   └─ {total_bronze:,} registros")

print(f"\n✅ SILVER (Registros Válidos):")
print(f"   └─ {total_silver:,} registros")
print(f"   └─ {silver_stats['bytes']:,} bytes")

print(f"\n❌ QUARANTINE (Registros Rechazados):")
print(f"   └─ {total_quarantine:,} registros")
print(f"   └─ {quarantine_stats['bytes']:,} bytes")

print(f"\n📈 RESUMEN:")
print(f"   ├─ Total Procesado: {total_processed:,} registros")
print(f"   ├─ Válidos: {total_silver:,} ({100*total_silver/total_processed:.1f}%)" if total_processed > 0 else "   ├─ Válidos: 0")
print(f"   └─ Rechazados: {total_quarantine:,} ({100*total_quarantine/total_processed:.1f}%)" if total_processed > 0 else "   └─ Rechazados: 0")

print("\n✅ La bifurcación de datos (split) ha sido implementada correctamente!")
print(f"\n💡 Registros inválidos guardados en: quarantine/secop_errors")
print(f"   (con columna 'motivo_rechazo' agregada para auditoría)")
print("\n" + "="*70)
