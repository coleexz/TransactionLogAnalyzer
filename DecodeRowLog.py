import pyodbc
from traceback import print_exc

def obtener_esquema_tabla(conexion, esquema, tabla):
    """
    Obtiene el esquema de la tabla desde la base de datos.
    Retorna una lista de columnas con su nombre, tipo, longitud, precisión y escala.
    """
    consulta = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION, NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{esquema}' AND TABLE_NAME = '{tabla}';
    """
    cursor = conexion.cursor()
    cursor.execute(consulta)
    esquema_tabla = cursor.fetchall()
    return [
        (fila.COLUMN_NAME, fila.DATA_TYPE, fila.CHARACTER_MAXIMUM_LENGTH, fila.NUMERIC_PRECISION, fila.NUMERIC_SCALE)
        for fila in esquema_tabla
    ]

def try_decode(data):
        """
        Detecta automáticamente la codificación de los datos.
        Decodifica en UTF-8 primero, pero si encuentra un patrón típico de UTF-16, cambia a UTF-16.
        """

        if all(data[i] == 0 for i in range(1, len(data), 2)):
            try:
                return data.decode("utf-16", errors="strict")
            except UnicodeDecodeError:
                pass

        try:
            return data.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")

def decode_rowlog(conexion, esquema, tabla, hex_data):
    """
    Decodifica un registro de RowLog Contents basado en el esquema de la tabla.
    """
    try:
        if isinstance(hex_data, str) and hex_data.startswith("0x"):
            hex_data = hex_data[2:]
        binary_data = bytes.fromhex(hex_data)

        print(f"DEBUG: Binary data completo: {binary_data}")

        # Obtener esquema de la tabla
        esquema_tabla = obtener_esquema_tabla(conexion, esquema, tabla)
        print(f"DEBUG: Esquema de tabla: {esquema_tabla}")

        # Separar columnas fijas y variables
        fixed_columns = [
            col for col in esquema_tabla if col[1].lower() not in ["varchar", "nvarchar", "text"]
        ]
        variable_columns = [
            col for col in esquema_tabla if col[1].lower() in ["varchar", "nvarchar", "text"]
        ]

        fixed_data_start = 4  # Saltar encabezado inicial
        decoded_columns = {}

        # Decodificar columnas fijas
        for col in fixed_columns:
            col_name, col_type, _, precision, scale = col

            if col_type.lower() == "int":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 4], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 4
            elif col_type.lower() == "decimal":
                if 1 <= precision <= 9:
                    bytes_for_value = 5
                elif 10 <= precision <= 19:
                    bytes_for_value = 9
                elif 20 <= precision <= 28:
                    bytes_for_value = 13
                elif 29 <= precision <= 38:
                    bytes_for_value = 17

                raw_value = binary_data[fixed_data_start + 1:fixed_data_start + bytes_for_value]
                is_negative = binary_data[fixed_data_start] & 0x80 != 0
                value = int.from_bytes(raw_value, byteorder="little")
                if is_negative:
                    value = -value
                decoded_columns[col_name] = value / (10 ** scale)
                fixed_data_start += bytes_for_value
            else:
                print(f"Tipo no manejado: {col_type}")
                fixed_data_start += 4

        relative_offsets: dict[int, int] = {}
        for idx, (col_name, data_type, _, _, _) in enumerate(variable_columns):
            # Obtener el offset inicial y procesar las columnas variables
            column_offset = int.from_bytes(binary_data[2:4], "little")
            print(f"DEBUG: Offset inicial: {column_offset}")

            total_columns = int.from_bytes(binary_data[column_offset:column_offset + 2], "little")
            print(f"DEBUG: Total columnas: {total_columns}")

            null_bitmap_size = (total_columns + 7) // 8
            variable_column_count_offset = column_offset + 2 + null_bitmap_size
            variable_column_count = int.from_bytes(
                binary_data[variable_column_count_offset:variable_column_count_offset + 2], "little"
            )
            print(f"DEBUG: Variable column count: {variable_column_count}")


            variable_data_start = (variable_column_count_offset + 2 + variable_column_count * 2)
            offset_start = variable_column_count_offset + 2
            print(f"DEBUG: Offset start: {offset_start}")

            # Leer offsets relativos

            for i in range(variable_column_count):
                s = offset_start + (i * 2)
                offset = int.from_bytes(
                    binary_data[s:s+2], "little"
                )
                relative_offsets[i]= offset

            print(f"DEBUG: Offsets relativos leídos: {relative_offsets}")

            last_offset = relative_offsets.get(idx-1)
            start = last_offset if last_offset else variable_data_start
            end = relative_offsets.get(idx) if idx < len(relative_offsets) else len(binary_data)

            print("Start: ", start)
            print("End: ", end)

            chunk = binary_data[start:end]
            val = try_decode(chunk)
            decoded_columns[col_name] = val

        return decoded_columns

    except Exception as e:
        print(f"Error al procesar RowLog Contents: {e}")
        print_exc()
        return None

if __name__ == "__main__":
    conexion = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;UID=sa;PWD=Pototo2005504;DATABASE=EmpleadosDB")
    hex_data = "0x300015000100000001F0490200000000000A00000005000002002D0038004C6170746F702044656C6C20585053456C65637472F36E696361"
    resultado = decode_rowlog(conexion, "Inventario", "Productos", hex_data)
    print("Resultado decodificado:")
    print(resultado)
