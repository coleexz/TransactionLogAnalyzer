import pyodbc
from ConnectDB import conectar_sqlserver


def obtener_esquema_tabla(conexion, esquema, tabla):
    """
    Obtiene el esquema de la tabla desde la base de datos.
    Retorna una lista de columnas con su nombre, tipo y longitud.
    """
    consulta = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{esquema}' AND TABLE_NAME = '{tabla}';
    """
    cursor = conexion.cursor()
    cursor.execute(consulta)
    esquema_tabla = cursor.fetchall()
    return [(fila.COLUMN_NAME, fila.DATA_TYPE, fila.CHARACTER_MAXIMUM_LENGTH) for fila in esquema_tabla]


def try_decode(data):
    """
    Detecta automáticamente la codificación de los datos.
    Decodifica en UTF-8 primero, pero si encuentra un patrón típico de UTF-16, cambia a UTF-16.
    """
    try:
        return data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        try:
            return data.decode("utf-16", errors="strict")
        except UnicodeDecodeError:
            return data.decode("latin-1", errors="replace")



def decode_rowlog(conexion, esquema, tabla, hex_data):
    """Decodifica un registro de RowLog Contents basado en el esquema de la tabla."""
    try:
        if isinstance(hex_data, str) and hex_data.startswith("0x"):
            hex_data = hex_data[2:]
        binary_data = bytes.fromhex(hex_data) if isinstance(hex_data, str) else hex_data

        print(f"DEBUG: Binary data completo: {binary_data}")

        esquema_tabla = obtener_esquema_tabla(conexion, esquema, tabla)

        # Dividir columnas según su tipo
        fixed_columns = [
            col for col in esquema_tabla if col[1].lower() not in ["varchar", "nvarchar", "text"]
        ]
        variable_columns = [
            col for col in esquema_tabla if col[1].lower() in ["varchar", "nvarchar", "text"]
        ]

        fixed_data_start = 4  # Saltar encabezado inicial
        decoded_columns = {}

        # Procesar columnas de tamaño fijo
        for col in fixed_columns:
            col_name, col_type, _ = col

            if col_type.lower() == "int":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 4], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 4  # Avanzar 4 bytes
            else:
                print(f"Tipo no manejado: {col_type}")

        # Procesar columnas de tamaño variable
        variable_data_start = fixed_data_start  # Inicio de datos variables

        for idx, col in enumerate(variable_columns):
            col_name, col_type, max_length = col

            if col_type.lower() in ["varchar", "nvarchar", "text"]:
                # Leer datos directamente
                start = variable_data_start
                print(f"DEBUG: Inspección manual desde {start}")

                # Leer un rango amplio para inspección
                inspect_data = binary_data[start:start + 16]
                print(f"DEBUG: Inspección de datos (16 bytes): {inspect_data}")

                # Intentar decodificar solo si hay caracteres imprimibles
                decoded_value = ""
                if any(b > 31 for b in inspect_data):
                    clean_start = next((i for i, b in enumerate(inspect_data) if b > 31), 0)
                    try:
                        decoded_value = inspect_data[clean_start:].decode("utf-8").strip()
                    except UnicodeDecodeError:
                        decoded_value = inspect_data[clean_start:].decode("latin-1").strip()

                decoded_columns[col_name] = decoded_value
                print(f"Columna '{col_name}': {decoded_value} (inspección manual desde {start})")

        return decoded_columns

    except Exception as e:
        print(f"Error al procesar RowLog Contents: {e}")
        return None



conexion = conectar_sqlserver("localhost", "1433", "sa", "Pototo2005504", "EmpleadosDB")
hexdata = "0x30000C00020000000E000000030000010017004F77656E"
resultado = decode_rowlog(conexion, "Estudiantes", "Estudiantes", hexdata)
print(resultado)
