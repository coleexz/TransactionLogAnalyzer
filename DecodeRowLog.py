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
    if all(data[i] == 0 for i in range(1, len(data), 2)):
        try:
            return data.decode('utf-16', errors='strict')
        except UnicodeDecodeError:
            pass

    try:
        return data.decode('utf-8', errors='strict')
    except UnicodeDecodeError:
        return data.decode('utf-8', errors='replace')

def decode_rowlog(conexion, esquema, tabla, hex_data):
    """Decodifica un registro de RowLog Contents basado en el esquema de la tabla."""
    try:

        if isinstance(hex_data, str) and hex_data.startswith("0x"):
            hex_data = hex_data[2:]  
        binary_data = bytes.fromhex(hex_data) if isinstance(hex_data, str) else hex_data


        esquema_tabla = obtener_esquema_tabla(conexion, esquema, tabla)

        status_bits = binary_data[:2]

        column_offset = int.from_bytes(binary_data[2:4], "little")

        if column_offset >= len(binary_data):
            print("Error: Offset al número de columnas fuera del rango de datos.")
            return None

        total_columns = int.from_bytes(binary_data[column_offset:column_offset + 2], "little")
        print(f"Total de Columnas: {total_columns}")

        null_bitmap_size = (total_columns + 7) // 8
        null_bitmap = binary_data[column_offset + 2:column_offset + 2 + null_bitmap_size]

        var_column_count_offset = column_offset + 2 + null_bitmap_size
        var_column_count = int.from_bytes(binary_data[var_column_count_offset:var_column_count_offset + 2], "little")
        var_offsets = []

        offset_start = var_column_count_offset + 2
        for i in range(var_column_count):
            var_offset = int.from_bytes(binary_data[offset_start + i * 2:offset_start + i * 2 + 2], "little")
            var_offsets.append(var_offset)

        decoded_columns = {}
        variable_data_start = offset_start + len(var_offsets) * 2
        variable_columns = [col for col in esquema_tabla if col[1].lower() in ["varchar", "nvarchar", "text"]]

        for idx, col in enumerate(variable_columns):
            col_name, col_type, col_length = col
            if idx < len(var_offsets):
                start = var_offsets[idx - 1] if idx > 0 else variable_data_start
                end = var_offsets[idx] if idx < len(var_offsets) else len(binary_data)
                if start < end and end <= len(binary_data):
                    # Detectar y decodificar la codificación adecuada
                    data_chunk = binary_data[start:end]
                    decoded_value = try_decode(data_chunk)
                    decoded_columns[col_name] = decoded_value.strip()
                    print(f"Columna '{col_name}': {decoded_value} (desde {start} hasta {end})")
                else:
                    print(f"Columna '{col_name}' tiene rangos inválidos (desde {start} hasta {end}).")

        return decoded_columns

    except Exception as e:
        print(f"Error al procesar RowLog Contents: {e}")
        return None

#conexion = conectar_sqlserver("localhost", "1433", "sa", "Pototo2005504", "EmpleadosDB")

#hexdata = "0x80584F"

#decode_rowlog(conexion, "RecursosHumanos", "Empleados", hexdata)
