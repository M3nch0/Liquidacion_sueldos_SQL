"""
Flujo del pipeline:
    1. EXTRACT   -> read csv file.
    2. LOAD      -> Insertar el DataFrame en tu base SQL Server.

    
"""
import pandas as pd
import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
import pyodbc


# ============================================================
# CONFIGURACION
# ============================================================
file_path = r"C:\Users\Usuario\OneDrive\Documentos\Mencho\DataCamp_curse\IngenieroDeDatosEnPython\airflow\carpeta1\empleados.csv"
CARPETA_SALIDA = os.path.join(os.path.dirname(os.path.dirname(file_path)), "carpeta2")

SERVER = "DESKTOP-28FG2P0"
DATABASE = "TestDB"
DRIVER = "{ODBC Driver 17 for SQL Server}"

# ============================================================
# 1. EXTRACT - Leer el CSV
# ============================================================

def read_employee_file_data(file_path):
    """
    To prepare the smartphone data for visualization, a number of
    transformations will be applied after reading in the raw DataFrame
    to memory, including:
        - reducing the number of columns to only those needed for
          later analysis
        - removing records without a battery_capacity or os value
        - dividing the price column by 100 to find the dollar amount

    :param file_path: the file path where the raw smartphone data
        is stored
    :return: a cleaned dataset having had the operations above
        applied to it
    """

    if os.path.exists(file_path):
        raw_data = pd.read_csv(file_path, sep=';')
    else:
        raise Exception(
            f"File containing employee data not found at path {file_path}"
        )
    return raw_data

# ============================================================
# 2. LOAD - Insertar el DataFrame en SQL Server (TestDB)
# ============================================================
def crear_base_si_no_existe(server, database, driver):
    """
    Se conecta a 'master' (que siempre existe) y crea la base
    solicitada si todavia no fue creada en el servidor.
    """
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE=master;Trusted_Connection=yes;",
        autocommit=True,
    )
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database}')
        CREATE DATABASE {database}
    """)
    conn.close()
    print(f"Base '{database}' lista ✅")


def cargar_a_sql(df_employees, server="DESKTOP-28FG2P0", database="TestDB"):
    """
    Crea (si no existe) la base y la tabla Pokemon, e inserta el DataFrame.
    Usa Windows Authentication, igual que en el resto de tus scripts.
    """
    driver = "{ODBC Driver 17 for SQL Server}"

    # Paso previo: asegurar que la base exista antes de conectarnos a ella
    crear_base_si_no_existe(server, database, driver)

    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;",
        autocommit=True,
    )
    cursor = conn.cursor()

    # Crear tabla si no existe
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stg_Empleados' AND xtype='U')
        CREATE TABLE stg_Empleados (
            id_empleado INT PRIMARY KEY,
            nombre VARCHAR(100),
            departamento VARCHAR(100),
            fecha_ingreso DATE,
            salario_base INT,
            evaluacion INT,
            horas_extra INT
        )
    """)
    print(f"Tabla 'stg_Empleados' lista en {database} ✅")
    
    # 🌟 NUEVA LÍNEA: Vacía la tabla antes de la inserción masiva para evitar duplicados
    cursor.execute("TRUNCATE TABLE stg_Empleados")
    print("Tabla 'stg_Empleados' vaciada correctamente 🧹")

    print(f"Insertando filas en la tabla 'stg_Empleados'...")
    df_employees['fecha_ingreso'] = pd.to_datetime(df_employees['fecha_ingreso'], dayfirst=True, errors='coerce')
    
    # 🌟 PASO 1: Aseguramos que la columna sea leída como fecha real por Pandas
    # errors='coerce' transforma cualquier texto inválido o vacío en NaT (nulo de Pandas)
    df_employees['fecha_ingreso'] = pd.to_datetime(df_employees['fecha_ingreso'], errors='coerce')

    # Función auxiliar rápida para manejar números vacíos
    def limpio_entero(valor):
        return None if pd.isna(valor) else int(valor)

    for index, fila in df_employees.iterrows():
        fecha_limpia = None if pd.isna(fila['fecha_ingreso']) else fila['fecha_ingreso'].strftime('%Y-%m-%d')
        
        try:
            cursor.execute("""
                INSERT INTO stg_Empleados (
                    id_empleado, nombre, departamento, fecha_ingreso, salario_base, evaluacion, horas_extra
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, 
                limpio_entero(fila['id_empleado']), 
                None if pd.isna(fila['nombre']) else str(fila['nombre']), 
                None if pd.isna(fila['departamento']) else str(fila['departamento']), 
                fecha_limpia,
                limpio_entero(fila['salario_base']), 
                limpio_entero(fila['evaluacion']),   # <-- ¡Acá ya no va a fallar si viene vacío!
                limpio_entero(fila['horas_extra'])
            )
        except Exception as e:
            print(f"❌ Error en la fila {index + 1} - ID {fila['id_empleado']}: Datos actuales -> Eval: {fila['evaluacion']}, Horas: {fila['horas_extra']}")
            raise e

    conn.commit()
    print("¡Datos cargados con éxito! 🎉")
    
# ============================================================
# 3. TRANSFORM - Pasos SQL (cada uno usa lo que calculo el anterior)
# ============================================================
# Valores de los parametros ya escritos en SQL
FECHA_LIQUIDACION = "2026-10-31"
PCT_APORTES = 17

transform_sql = [
    ("Paso 0 - borrar liquidacion anterior", "DROP TABLE IF EXISTS liquidacion;"),
 
    # Paso 1: copia de los datos ya limpios. Las columnas con CAST(NULL AS tipo)
    # se crean vacias, con su tipo, y se llenan en los pasos siguientes.
    ("Paso 1 - copia limpia de stg_Empleados", f"""
        SELECT id_empleado,
        LTRIM(nombre) AS nombre,
        UPPER(LTRIM(RTRIM(departamento))) AS departamento,
        CAST(NULL AS VARCHAR(150)) AS email,
        COALESCE(evaluacion, 3) AS evaluacion,
        CAST(salario_base as decimal(10,2)) AS salario_base,
        COALESCE(horas_extra, 0) AS horas_extra,
        DATEDIFF(YEAR, fecha_ingreso, {FECHA_LIQUIDACION})
                 - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, fecha_ingreso, {FECHA_LIQUIDACION}), fecha_ingreso)
                 > {FECHA_LIQUIDACION} THEN 1 ELSE 0 END AS antiguedad,
                 CAST(NULL AS DECIMAL(12,2)) AS bono_antiguedad,
               CAST(NULL AS DECIMAL(12,2)) AS bono_evaluacion,
               CAST(NULL AS DECIMAL(12,2)) AS pago_horas_extra,
               CAST(NULL AS DECIMAL(13,2)) AS salario_bruto,
               CAST(NULL AS DECIMAL(12,2)) AS aportes,
               CAST(NULL AS DECIMAL(13,2)) AS salario_neto,
               CAST(NULL AS INT) AS cuartil_neto
        INTO liquidacion
        FROM stg_Empleados
    """),
    ("Paso 2a - email", """
        UPDATE liquidacion
        SET email = LOWER(REPLACE(nombre, ' ', '.')) + '@empresa.com';
    """),
 
    ("Paso 2b - nombre con formato", """
        UPDATE liquidacion
        SET nombre = UPPER(LEFT(nombre, 1))
                   + LOWER(SUBSTRING(nombre, 2, CHARINDEX(' ', nombre) - 1))
                   + UPPER(SUBSTRING(nombre, CHARINDEX(' ', nombre) + 1, 1))
                   + LOWER(SUBSTRING(nombre, CHARINDEX(' ', nombre) + 2, 100));
    """),
    ("Paso 3 - bonos y horas extra", """
        UPDATE liquidacion
        SET bono_antiguedad  = ROUND(salario_base *
                                 CASE WHEN antiguedad_anios * 2 > 20 THEN 20 ELSE antiguedad_anios * 2 END
                                 / 100.0, 2),
            bono_evaluacion  = ROUND(salario_base *
                                 CASE evaluacion WHEN 5 THEN 15 WHEN 4 THEN 10 WHEN 3 THEN 5 ELSE 0 END
                                 / 100.0, 2),
            pago_horas_extra = ROUND(horas_extra * (salario_base / 160.0) * 1.5, 2);
    """),
 
    ("Paso 4a - salario bruto", """
        UPDATE liquidacion
        SET salario_bruto = salario_base + bono_antiguedad + bono_evaluacion + pago_horas_extra;
    """),
 
    ("Paso 4b - aportes", f"""
        UPDATE liquidacion
        SET aportes = ROUND(salario_bruto * {PCT_APORTES} / 100.0, 2);
    """),
 
    ("Paso 4c - salario neto", """
        UPDATE liquidacion
        SET salario_neto = salario_bruto - aportes;
    """),
 
    ("Paso 5 - cuartil segun el neto", """
        UPDATE l
        SET cuartil_neto = q.cuartil
        FROM liquidacion l
        JOIN (SELECT id_empleado, NTILE(4) OVER (ORDER BY salario_neto) AS cuartil
              FROM liquidacion) q
          ON q.id_empleado = l.id_empleado;
    """),
 
    ("Paso 6a - borrar reporte anterior", "DROP TABLE IF EXISTS reporte_departamentos;"),
 
    ("Paso 6b - reporte por departamento", """
        SELECT l.departamento,
               COUNT(*) AS cantidad_empleados,
               SUM(l.salario_bruto) AS masa_salarial_bruta,
               CAST(ROUND(AVG(l.salario_neto), 2) AS DECIMAL(12,2)) AS neto_promedio,
               CAST(ROUND(SUM(l.salario_bruto) * 100.0 /
                          (SELECT SUM(salario_bruto) FROM liquidacion), 1) AS DECIMAL(5,1)) AS participacion_pct,
               MAX(l.salario_neto) AS mayor_neto,
               (SELECT TOP 1 x.nombre
                FROM liquidacion x
                WHERE x.departamento = l.departamento
                ORDER BY x.salario_neto DESC) AS empleado_mayor_neto
        INTO reporte_departamentos
        FROM liquidacion l
        GROUP BY l.departamento;
    """),]

def transformar_datos(conn, sql_statements):
    """
    Ejecuta una lista de sentencias SQL en la conexión proporcionada.
    """
    cursor = conn.cursor()
    for sql in sql_statements:
        cursor.execute(sql)
        conn.commit()
    print("Transformaciones aplicadas con éxito ✅")

# ============================================================
# 4. VALIDAR
# ============================================================
def validar_datos(conn):
    """
    Realiza validaciones básicas sobre la tabla 'liquidacion' y el reporte.
    """
    cursor = conn.cursor()
    
    # Validación 1: Verificar que no haya registros con salario_neto negativo
    cursor.execute("SELECT COUNT(*) FROM liquidacion WHERE salario_neto < 0;")
    negativos = cursor.fetchone()[0]
    if negativos > 0:
        print(f"❌ Advertencia: Hay {negativos} registros con salario_neto negativo.")
    else:
        print("✅ Todos los registros tienen salario_neto positivo.")

    # Validación 2: Verificar que el reporte_departamentos tenga datos
    cursor.execute("SELECT COUNT(*) FROM reporte_departamentos;")
    total_departamentos = cursor.fetchone()[0]
    if total_departamentos == 0:
        print("❌ Advertencia: El reporte_departamentos está vacío.")
    else:
        print(f"✅ El reporte_departamentos contiene {total_departamentos} departamentos.")

# ============================================================
# 5. EXPORTAR a carpeta2
# ============================================================
def exportar_tabla(conn, consulta, ruta):
    """
    Exporta el resultado de una consulta SQL a un archivo CSV en la ruta especificada.
    """
    cursor = conn.cursor()
    cursor.execute(consulta)
    columnas = [column[0] for column in cursor.description]
    filas = [tuple(row) for row in cursor.fetchall()]
    pd.DataFrame(filas, columns=columnas).to_csv(ruta, index=False, encoding='utf-8-sig')
    print(f"Exportando: {ruta} ({len(filas)} filas)✅")

def exportar(conn):
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    exportar_tabla(
        conn,
        "SELECT * FROM liquidacion ORDER BY id_empleado",
        os.path.join(CARPETA_SALIDA, f"liquidacion_{FECHA_LIQUIDACION}.csv"),
    )
    exportar_tabla(
        conn,
        "SELECT * FROM reporte_departamentos ORDER BY departamento",
        os.path.join(CARPETA_SALIDA, "reporte_departamentos.csv"),
    )

# ============================================================
# EJECUCION DEL PIPELINE COMPLETO
# ============================================================
if __name__ == "__main__":
    df_employees = read_employee_file_data(file_path)
    print(f"\nTotal empleados en el CSV: {len(df_employees)}")

    crear_base_si_no_existe(SERVER, DATABASE, DRIVER)
    conn = pyodbc.connect(f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;", autocommit=True)
    try:
        filas_csv = cargar_a_sql(conn, df_employees)
        transformar_datos(conn)
        validar_datos(conn)
        exportar(conn)
    finally:
        conn.close()
    print("\n🎉 Pipeline ETL completo: API -> pandas -> SQL Server")