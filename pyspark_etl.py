"""
ETL CON PYSPARK
Online Retail Dataset 

"""
import os
import glob
import shutil
import time
import warnings

# ============================================================
# 0. CONFIGURACION HADOOP_HOME (Windows) + verificacion winutils
# ============================================================
_hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".hadoop")
os.makedirs(os.path.join(_hadoop_home, "bin"), exist_ok=True)
os.environ["HADOOP_HOME"] = _hadoop_home
os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

_winutils_path = os.path.join(_hadoop_home, "bin", "winutils.exe")
if not os.path.isfile(_winutils_path):
    print("=" * 60)
    print("[AVISO IMPORTANTE] No se encontro winutils.exe")
    print("=" * 60)
    print(f"Ruta esperada: {_winutils_path}")
    print("Esta es la causa mas probable de:")
    print("  - 'Failed to delete file or dir ..._temporary'")
    print("  - Bloqueos/colgadas durante la escritura de CSV")
    print("  - Py4JNetworkError tras un KeyboardInterrupt manual")
    print("")
    print("Solucion: descargue winutils.exe + hadoop.dll (Hadoop 3.3.x,")
    print("compatible con Spark 3.5/4.x) desde un repositorio confiable, ej:")
    print("  https://github.com/kontext-tech/winutils")
    print(f"y coloquelos en: {os.path.join(_hadoop_home, 'bin')}")
    print("=" * 60)

# El warning de pandas >= 3.0 lo emite PySpark al importar, sin que el
# ETL use pandas realmente (no hay toPandas() ni createDataFrame desde
# pandas en este script). Se silencia puntualmente para no ensuciar la
# salida; la recomendacion real (pandas < 3.0.0) va en las instrucciones.
warnings.filterwarnings("ignore", message=".*pandas.*")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum, avg, min, max, count, countDistinct,
    regexp_replace, round as spark_round, lit, substring
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURACION DE SPARK
# ============================================================
spark = (
    SparkSession.builder
    .appName("ETL_OnlineRetail")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    # Reduce operaciones de archivo durante el commit (menos roces con
    # el bloqueo de archivos de Windows / antivirus en tiempo real)
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version: {spark.version}")
print(f"Workers/nucleos: {spark.sparkContext.defaultParallelism}")

# ============================================================
# 2. RUTAS
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data/Online_Retail.csv")
RESULTADOS_DIR = os.path.join(BASE_DIR, "resultados")
os.makedirs(RESULTADOS_DIR, exist_ok=True)

if "onedrive" in BASE_DIR.lower() or "dropbox" in BASE_DIR.lower():
    print("[AVISO] El proyecto esta dentro de una carpeta sincronizada")
    print("        (OneDrive/Dropbox). Estos servicios pueden bloquear")
    print("        archivos recien creados y causar fallos de escritura.")
    print("        Si los problemas persisten, mueva el proyecto fuera")
    print("        de la carpeta sincronizada.")

print("\n" + "=" * 60)
print("INICIO DEL PROCESO ETL")
print("=" * 60)

# ============================================================
# 3. LECTURA DEL DATASET
# ============================================================
print("\n[1/5] Lectura del dataset...")

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(DATA_PATH)
)

total_raw = df_raw.count()
print(f"[OK] {total_raw} registros cargados")

print("\n=== ESQUEMA BRUTO (antes de limpieza) ===")
df_raw.printSchema()
print("\n=== PRIMERAS 5 FILAS ===")
df_raw.show(5)

# ============================================================
# 4. LIMPIEZA Y TRANSFORMACIONES
# ============================================================
print("\n[2/5] Limpieza y transformacion...")

df = df_raw.withColumn("Quantity", col("Quantity").cast("integer"))
df = df.withColumn("UnitPrice",
    regexp_replace(col("UnitPrice"), ",", ".").cast("double"))
df = df.withColumn("CustomerID", col("CustomerID").cast("integer"))

# --- Fechas (formato europeo dd/MM/yyyy H:mm, no lo parsea bien Spark) ---
df = df.withColumn("fecha_dia", substring(col("InvoiceDate"), 1, 2).cast("integer"))
df = df.withColumn("mes", substring(col("InvoiceDate"), 4, 2).cast("integer"))
df = df.withColumn("ano", substring(col("InvoiceDate"), 7, 4).cast("integer"))
df = df.withColumn("fecha_iso",
    F.concat(
        substring(col("InvoiceDate"), 7, 4), lit("-"),
        substring(col("InvoiceDate"), 4, 2), lit("-"),
        substring(col("InvoiceDate"), 1, 2)
    )
)

meses = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
         7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
mes_expr = lit("")
for m_num, m_nombre in meses.items():
    mes_expr = when(col("mes") == m_num, m_nombre).otherwise(mes_expr)
df = df.withColumn("mes_nombre", mes_expr)

# --- TotalAmount como DecimalType(18,2) ---
# Antes: Quantity(int) * UnitPrice(double) -> arrastra error de coma
# flotante (ej. 279489.01999999996). DecimalType(18,2) da aritmetica
# exacta en base 10 y evita ese ruido sin cambiar el resultado real
# (279489.02 sigue siendo 279489.02, solo que representado con exactitud).
df = df.withColumn(
    "TotalAmount",
    spark_round((col("Quantity") * col("UnitPrice")).cast(DecimalType(18, 2)), 2)
)
df = df.withColumn("es_devolucion", col("Quantity") < 0)

# --- Cache: df se reutiliza en ~15 acciones a continuacion (counts,
# filtros, agregaciones, join, ranking). Sin cache, cada una de esas
# acciones vuelve a leer el CSV y rehacer todos los casts/fechas desde
# cero. Se libera con df.unpersist() al final, cuando ya no se necesita.
df.cache()
df.count()  # fuerza la materializacion del cache ahora, no a mitad del analisis

print("[OK] Transformaciones completadas")

print("\n=== ESQUEMA DESPUES DE CAST ===")
df.printSchema()

print("\n=== VALORES NULOS POR COLUMNA (columnas clave) ===")
df.select([count(col(c)).alias(c) for c in ["CustomerID", "Description", "Quantity", "UnitPrice"]]).show(truncate=False)

nulos_customer = df.filter(col("CustomerID").isNull()).count()
devoluciones = df.filter(col("Quantity") < 0).count()
precio_cero = df.filter(col("UnitPrice") <= 0).count()
print(f"CustomerID nulo:     {nulos_customer} ({nulos_customer * 100 // total_raw}%)")
print(f"Quantity negativa:   {devoluciones}")
print(f"UnitPrice <= 0:      {precio_cero}")

print("\n=== TotalAmount (primeras 5 filas) ===")
df.select("InvoiceNo", "Quantity", "UnitPrice", "TotalAmount", "es_devolucion").show(5)

# ============================================================
# 5. DATAFRAMES AUXILIARES PARA JOIN
# ============================================================
df_clientes = (
    df.filter(col("CustomerID").isNotNull())
    .groupBy("CustomerID", "Country")
    .agg(
        sum("TotalAmount").alias("total_compras"),
        countDistinct("InvoiceNo").alias("total_facturas"),
        count("*").alias("total_lineas")
    )
)

df_facturas_cliente = (
    df.filter(col("CustomerID").isNotNull() & (col("Quantity") > 0))
    .groupBy("CustomerID")
    .agg(
        avg("TotalAmount").alias("ticket_promedio_linea"),
        max("TotalAmount").alias("compra_maxima_linea"),
        sum("Quantity").alias("total_unidades")
    )
)

df_clientes_completo = df_clientes.join(df_facturas_cliente, on="CustomerID", how="inner")

print("\n=== DataFrame clientes + facturas (JOIN) ===")
df_clientes_completo.show(5, truncate=False)

# ============================================================
# 6. ANALISIS - LAS 10 PREGUNTAS
# ============================================================
print("\n[3/5] Analisis...")

# --- P1: Total de facturas ---
total_registros = total_raw
total_facturas = df.select("InvoiceNo").distinct().count()
r1 = spark.createDataFrame(
    [("total_registros", total_registros), ("total_facturas_unicas", total_facturas)],
    ["metrica", "valor"]
)

# --- P2: Clientes unicos ---
clientes_con_id = df.select(countDistinct("CustomerID")).collect()[0][0]
r2 = spark.createDataFrame([("clientes_unicos", clientes_con_id)], ["metrica", "valor"])

# --- P3: Ingreso total ---
ingreso_con_dev = df.agg(sum("TotalAmount")).collect()[0][0]
ingreso_sin_dev = df.filter(col("Quantity") > 0).agg(sum("TotalAmount")).collect()[0][0]
ingreso_dev = df.filter(col("Quantity") < 0).agg(sum("TotalAmount")).collect()[0][0]
r3 = spark.createDataFrame(
    [("ingreso_total_neto", float(ingreso_con_dev)),
     ("ingreso_ventas", float(ingreso_sin_dev)),
     ("ingreso_devoluciones", float(ingreso_dev))],
    ["metrica", "valor"]
)

# --- P4: Producto mas vendido ---
r4 = (
    df.filter(col("Quantity") > 0)
    .groupBy("StockCode", "Description")
    .agg(sum("Quantity").alias("cantidad_vendida"))
    .orderBy(col("cantidad_vendida").desc())
    .limit(10)
)

# --- P5: Cliente con mayor compra ---
r5 = (
    df.filter(col("CustomerID").isNotNull())
    .groupBy("CustomerID")
    .agg(sum("TotalAmount").alias("total_compra"))
    .orderBy(col("total_compra").desc())
    .limit(10)
)

# --- P6: Top 5 paises fuera de UK (metrica: ingreso monetario) ---
r6 = (
    df.filter((col("Country") != "United Kingdom") & (col("Quantity") > 0))
    .groupBy("Country")
    .agg(
        sum("TotalAmount").alias("ingreso_total"),
        countDistinct("InvoiceNo").alias("num_facturas")
    )
    .orderBy(col("ingreso_total").desc())
    .limit(5)
)

# --- P7: Ticket promedio por factura ---
# sum(TotalAmount) agrupado por InvoiceNo, luego avg() de esos totales.
# NO es avg(UnitPrice): eso seria precio por unidad, no gasto por factura.
ticket_data = (
    df.filter(col("Quantity") > 0)
    .groupBy("InvoiceNo")
    .agg(sum("TotalAmount").alias("total_factura"))
)
ticket_promedio = ticket_data.agg(spark_round(avg("total_factura"), 2)).collect()[0][0]
ticket_min = ticket_data.agg(min("total_factura")).collect()[0][0]
ticket_max = ticket_data.agg(max("total_factura")).collect()[0][0]
r7 = spark.createDataFrame(
    [("ticket_promedio", float(ticket_promedio)),
     ("ticket_minimo", float(ticket_min)),
     ("ticket_maximo", float(ticket_max))],
    ["metrica", "valor"]
)

# --- P8: Unidades por factura (min/max/promedio) ---
# sum("Quantity") por factura = unidades totales por factura, NO conteo
# de productos distintos. El nombre de variable se corrigio para
# reflejar esto; el calculo y el resultado no cambiaron.
unidades_por_factura = (
    df.filter(col("Quantity") > 0)
    .groupBy("InvoiceNo")
    .agg(sum("Quantity").alias("total_unidades_factura"))
)
min_unid = unidades_por_factura.agg(min("total_unidades_factura")).collect()[0][0]
max_unid = unidades_por_factura.agg(max("total_unidades_factura")).collect()[0][0]
avg_unid = unidades_por_factura.agg(spark_round(avg("total_unidades_factura"), 2)).collect()[0][0]
r8 = spark.createDataFrame(
    [("min_unidades_por_factura", float(min_unid)),
     ("max_unidades_por_factura", float(max_unid)),
     ("avg_unidades_por_factura", float(avg_unid))],
    ["metrica", "valor"]
)

# --- P9: Mes con mas ventas ---
r9 = (
    df.filter(col("Quantity") > 0)
    .groupBy("mes", "mes_nombre")
    .agg(
        sum("TotalAmount").alias("ingreso_total"),
        countDistinct("InvoiceNo").alias("num_facturas")
    )
    .orderBy(col("ingreso_total").desc())
)
mes_top = r9.limit(1).collect()[0]

# --- P10: Porcentaje de facturas con devoluciones ---
facturas_con_dev = df.filter(col("Quantity") < 0).select("InvoiceNo").distinct().count()
pct_devoluciones = round((facturas_con_dev / total_facturas) * 100, 2)
r10 = spark.createDataFrame(
    [("facturas_con_devolucion", float(facturas_con_dev)),
     ("total_facturas", float(total_facturas)),
     ("porcentaje_devoluciones", float(pct_devoluciones))],
    ["metrica", "valor"]
)

print("[OK] Preguntas analizadas")
print(f"\nResumen rapido:")
print(f"  Facturas unicas: {total_facturas} | Clientes: {clientes_con_id}")
print(f"  Ingreso neto: ${ingreso_con_dev:,.2f}")
print(f"  Ticket promedio: ${ticket_promedio:,.2f}")
print(f"  Mes top: {mes_top['mes_nombre']} (${mes_top['ingreso_total']:,.2f})")
print(f"  Devoluciones: {pct_devoluciones}%")

# ============================================================
# 7. FUNCION DE VENTANA: Ranking de clientes
# ============================================================
# Window.orderBy() SIN partitionBy() es correcto aqui: el ranking pedido
# es GLOBAL (no hay una clave logica por la que dividir clientes en
# grupos independientes). El warning "No Partition Defined" es Spark
# advirtiendo que movera todo a una particion -- es el comportamiento
# esperado y necesario para que rank() de un orden total consistente,
# no un error a corregir. Con 4372 clientes el costo es despreciable.
window_spec = Window.orderBy(col("total_compra").desc())

ranking_clientes = (
    df.filter(col("CustomerID").isNotNull())
    .groupBy("CustomerID")
    .agg(
        sum("TotalAmount").alias("total_compra"),
        countDistinct("InvoiceNo").alias("num_facturas")
    )
    .withColumn("rank", F.rank().over(window_spec))
    .withColumn("row_number", F.row_number().over(window_spec))
    .orderBy(col("rank"))
    .limit(10)
)
print("\n=== Top 10 clientes por volumen de compra ===")
ranking_clientes.show(truncate=False)

# ============================================================
# 8. EXPORTACION DE RESULTADOS
# ============================================================
print("\n[4/5] Exportacion...")

def exportar(df_result, nombre_archivo, reintentos=3):
    """
    Exporta un DataFrame a UN SOLO archivo CSV limpio:
    resultados/<nombre_archivo>.csv

    Spark solo sabe escribir CSV dentro de una carpeta (genera un
    part-*.csv real + _SUCCESS + .crc junto a el; eso no es
    configurable en el writer). Por eso se escribe a una carpeta
    temporal interna, se rescata el part-*.csv real y se mueve al
    nombre final; la carpeta temporal (con _SUCCESS/.crc incluidos)
    se descarta despues. El resultado es un unico .csv por pregunta,
    sin nada mas alrededor.

    - Limpia restos de corridas anteriores antes de escribir.
    - Reintenta con backoff ante bloqueos transitorios de archivo
      (antivirus, indexado, sincronizacion en la nube).
    - No oculta errores reales: si tras los reintentos sigue fallando,
      relanza la excepcion original con contexto.
    """
    ruta_final = os.path.join(RESULTADOS_DIR, f"{nombre_archivo}.csv")
    ruta_temp = os.path.join(RESULTADOS_DIR, f"_tmp_{nombre_archivo}")

    for intento in range(1, reintentos + 1):
        if os.path.exists(ruta_temp):
            shutil.rmtree(ruta_temp, ignore_errors=True)
        try:
            (df_result.coalesce(1)
             .write.mode("overwrite")
             .option("header", "true")
             .csv(ruta_temp))

            partes = glob.glob(os.path.join(ruta_temp, "part-*.csv"))
            if not partes:
                raise RuntimeError(f"Spark no genero part-*.csv en {ruta_temp}")

            if os.path.exists(ruta_final):
                os.remove(ruta_final)
            shutil.move(partes[0], ruta_final)
            shutil.rmtree(ruta_temp, ignore_errors=True)

            print(f"  [OK] {nombre_archivo}.csv")
            return
        except Exception as e:
            print(f"  [WARN] Intento {intento}/{reintentos} fallo en '{nombre_archivo}': {e}")
            if intento == reintentos:
                print(f"  [ERROR] No se pudo exportar '{nombre_archivo}' tras {reintentos} intentos.")
                raise
            if os.path.exists(ruta_temp):
                shutil.rmtree(ruta_temp, ignore_errors=True)
            time.sleep(2 * intento)

exportar(r1, "pregunta_01_total_facturas")
exportar(r2, "pregunta_02_clientes_unicos")
exportar(r3, "pregunta_03_ingreso_total")
exportar(r4, "pregunta_04_producto_mas_vendido")
exportar(r5, "pregunta_05_cliente_mayor_compra")
exportar(r6, "pregunta_06_top_5_paises")
exportar(r7, "pregunta_07_ticket_promedio")
exportar(r8, "pregunta_08_productos_por_factura")
exportar(r9, "pregunta_09_ventas_por_mes")
exportar(r10, "pregunta_10_porcentaje_devoluciones")
exportar(ranking_clientes, "ranking_clientes")
exportar(df_clientes_completo, "clientes_con_join")

print(f"[OK] Resultados exportados a: {RESULTADOS_DIR}")

# ============================================================
# 9. CIERRE
# ============================================================
print("\n[5/5] Finalizacion...")
df.unpersist()
spark.stop()
print("[OK] Spark detenido correctamente")

print("\n" + "=" * 60)
print("ETL FINALIZADO CORRECTAMENTE")
print("=" * 60)