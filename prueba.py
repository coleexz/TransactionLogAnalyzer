import pyodbc

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
    """Decodifica un registro de RowLog Contents basado en el esquema de la tabla, con detección automática de codificación."""
    try:
        if hex_data.startswith("0x"):
            hex_data = hex_data[2:]


        esquema_tabla = obtener_esquema_tabla(conexion, esquema, tabla)

        binary_data = bytes.fromhex(hex_data)

        status_bits = binary_data[:2]
        print(f"Status Bits: {status_bits.hex()}")

        # Leer Offset al Número de Columnas
        column_offset = int.from_bytes(binary_data[2:4], "little")
        print(f"Offset al Número de Columnas: {column_offset}")

        if column_offset >= len(binary_data):
            print("Error: Offset al número de columnas fuera del rango de datos.")
            return

        total_columns = int.from_bytes(binary_data[column_offset:column_offset + 2], "little")
        print(f"Total de Columnas: {total_columns}")

        null_bitmap_size = (total_columns + 7) // 8
        null_bitmap = binary_data[column_offset + 2:column_offset + 2 + null_bitmap_size]
        print(f"Bitmap de Nulabilidad: {null_bitmap.hex()}")

        var_column_count_offset = column_offset + 2 + null_bitmap_size
        if var_column_count_offset + 2 > len(binary_data):
            print("Error: Datos insuficientes para leer el número de columnas de longitud variable.")
            return

        var_column_count = int.from_bytes(binary_data[var_column_count_offset:var_column_count_offset + 2], "little")
        print(f"Número de Columnas de Longitud Variable: {var_column_count}")

        var_offsets = []
        offset_start = var_column_count_offset + 2
        for i in range(var_column_count):
            if offset_start + i * 2 + 2 > len(binary_data):
                print("Error: Datos insuficientes para leer offsets de columnas variables.")
                return
            var_offset = int.from_bytes(binary_data[offset_start + i * 2:offset_start + i * 2 + 2], "little")
            var_offsets.append(var_offset)
        print(f"Offsets de Columnas Variables: {var_offsets}")

        if len(var_offsets) != var_column_count:
            print("Error: Los offsets de columnas variables no coinciden con el número de columnas variables.")
            return

        variable_data_start = offset_start + len(var_offsets) * 2

        decoded_columns = {}
        variable_columns = [col for col in esquema_tabla if col[1].lower() in ["varchar", "nvarchar", "text"]]

        for idx, col in enumerate(variable_columns):
            col_name, col_type, col_length = col
            if idx < len(var_offsets):
                start = var_offsets[idx - 1] if idx > 0 else variable_data_start
                end = var_offsets[idx] if idx < len(var_offsets) else len(binary_data)
                if start < end and end <= len(binary_data):
                    decoded_value = try_decode(binary_data[start:end])
                    decoded_columns[col_name] = decoded_value
                    print(f"Columna '{col_name}': {decoded_value} (desde {start} hasta {end})")
                else:
                    print(f"Columna '{col_name}' tiene rangos inválidos (desde {start} hasta {end}).")
            else:
                print(f"Columna '{col_name}' no tiene un offset asignado y será ignorada.")

        print(f"Decoded Columns: {decoded_columns}")
        return decoded_columns

    except Exception as e:
        print(f"Error al procesar RowLog Contents: {e}")


conexion = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=EmpleadosDB;'
    'UID=sa;'
    'PWD=Pototo2005504;'
    'TrustServerCertificate=yes;'
)

rowlog_hex = "30001100230000000120AA4400000000000500000300210026003800C16E67656C526F6A617353757065727669736F7220646520C1726561"
decode_rowlog(conexion, "RecursosHumanos","Empleados", rowlog_hex)
