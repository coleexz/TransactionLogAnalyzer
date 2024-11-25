import pyodbc
import struct
from traceback import print_exc
import datetime



def obtener_esquema_tabla(conexion, esquema, tabla):
    """
    Obtiene el esquema de la tabla desde la base de datos.
    Retorna una lista de columnas con su nombre, tipo, longitud, precisión y escala.
    """
    consulta = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{esquema}' AND TABLE_NAME = '{tabla}';
    """
    cursor = conexion.cursor()
    cursor.execute(consulta)
    esquema_tabla = cursor.fetchall()
    return [
        (fila.COLUMN_NAME, fila.DATA_TYPE, fila.CHARACTER_MAXIMUM_LENGTH, fila.NUMERIC_PRECISION, fila.NUMERIC_SCALE, fila.CHARACTER_MAXIMUM_LENGTH, fila.CHARACTER_OCTET_LENGTH)
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

        # Normalizar hex_data según su tipo
        if isinstance(hex_data, bytes):
            hex_data = hex_data.hex()  # Convierte bytes a cadena hexadecimal
        elif isinstance(hex_data, str) and hex_data.startswith("0x"):
            hex_data = hex_data[2:]  # Elimina prefijo "0x"

        # Validar que hex_data sea una cadena hexadecimal válida
        if not all(c in "0123456789abcdefABCDEF" for c in hex_data):
            raise ValueError(f"Datos no válidos: {hex_data}")

        # Convertir a binario
        binary_data = bytes.fromhex(hex_data)
        print(f"DEBUG: Datos binarios procesados: {binary_data}")


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
            col_name, col_type, _, precision, scale, max_length, octet_length = col

            if col_type.lower() == "int":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 4], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 4
            elif col_type.lower() == "smallint":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 2], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 2
            elif col_type.lower() == "tinyint":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 1], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 1
            elif col_type.lower() == "bigint":
                value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 8], "little")
                decoded_columns[col_name] = value
                fixed_data_start += 8
            elif col_type.lower() == "real":
                value = struct.unpack("<f", binary_data[fixed_data_start:fixed_data_start + 4])[0]
                exact_value = format(value, ".7g")
                decoded_columns[col_name] = float(exact_value)
                fixed_data_start += 4
            elif col_type.lower() == "float":
                value = struct.unpack("<d", binary_data[fixed_data_start:fixed_data_start + 8])[0]
                exact_value = format(value, ".15g")
                decoded_columns[col_name] = float(exact_value)
                fixed_data_start += 8
            elif col_type.lower() == "char":
                # Determinar la longitud exacta
                length_in_bytes = next((col[6] for col in esquema_tabla if col[0] == col_name), None)
                if length_in_bytes is None:
                    raise ValueError(f"No se pudo determinar la longitud de {col_name}")
                value = binary_data[fixed_data_start:fixed_data_start + length_in_bytes].decode("latin1").strip()
                decoded_columns[col_name] = value
                fixed_data_start += length_in_bytes
            elif col_type.lower() == "money":
                value = struct.unpack("<q", binary_data[fixed_data_start:fixed_data_start + 8])[0] / 10000.0
                decoded_columns[col_name] = value
                fixed_data_start += 8
            elif col_type.lower() == "smallmoney":
                value = struct.unpack("<i", binary_data[fixed_data_start:fixed_data_start + 4])[0] / 10000.0
                decoded_columns[col_name] = value
                fixed_data_start += 4
            elif col_type.lower() == "nchar":
                # Usar CHARACTER_OCTET_LENGTH para obtener longitud en bytes
                length_in_bytes = next(
                    (col[6] for col in esquema_tabla if col[0] == col_name), None  # Col[6] corresponde a CHARACTER_OCTET_LENGTH
                )
                if length_in_bytes is None:
                    raise ValueError(f"No se pudo determinar la longitud de {col_name}")
                value = binary_data[fixed_data_start:fixed_data_start + length_in_bytes].decode("utf-16le", errors="ignore")
                decoded_columns[col_name] = value.strip()  # Remover espacios adicionales
                fixed_data_start += length_in_bytes
            elif col_type.lower() == "binary":
                length_in_bytes = next(
                    (col[6] for col in esquema_tabla if col[0] == col_name), None
                )
                if length_in_bytes is None:
                    raise ValueError(f"No se pudo determinar la longitud de {col_name}")
                value = binary_data[fixed_data_start:fixed_data_start + length_in_bytes]
                hex_representation = f"0x{value.hex().upper()}"
                decoded_columns[col_name] = hex_representation
                fixed_data_start += length_in_bytes
                precision = col[4] or 0  # Usar precisión predeterminada si no está definida
                scale_multiplier = 10 ** precision
                raw_value = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 3], "little")
                hours, remainder = divmod(raw_value // scale_multiplier, 3600)
                minutes, seconds = divmod(remainder, 60)
                fractional_seconds = (raw_value % scale_multiplier) / scale_multiplier
                value = f"{hours:02}:{minutes:02}:{seconds:02}.{int(fractional_seconds * scale_multiplier)}"
                decoded_columns[col_name] = value
                fixed_data_start += 3
            elif col_type.lower() == "rowversion":
                value = binary_data[fixed_data_start:fixed_data_start + 8].hex().upper()
                decoded_columns[col_name] = value
                fixed_data_start += 8
            elif col_type.lower() == "numeric":
                precision = col[3]
                scale = col[4]
                if precision is None or scale is None:
                    raise ValueError(f"Precisión o escala no definida para {col_name}")

                if precision <= 9:
                    bytes_for_value = 5
                elif precision <= 19:
                    bytes_for_value = 9
                elif precision <= 28:
                    bytes_for_value = 13
                else:
                    bytes_for_value = 17

                raw_value = binary_data[fixed_data_start + 1:fixed_data_start + bytes_for_value]
                is_negative = binary_data[fixed_data_start] & 0x80 != 0
                value = int.from_bytes(raw_value, byteorder="little")
                if is_negative:
                    value = -value
                decoded_columns[col_name] = value / (10 ** scale)
                fixed_data_start += bytes_for_value
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
        for idx, (col_name, data_type, _, _, _,_,_) in enumerate(variable_columns):
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
    hex_data = "0x10004B00546578746F3132333420351CDCDF020000000061BC00013FB49600000000008E470B53BD1C037405EF0033B20000078CA392798E470BD3CEE5028E470B3C0000000000000007D70A000000"
    resultado = decode_rowlog(conexion, "dbo", "TiposEspeciales", hex_data)
    print("Resultado decodificado:")
    print(resultado)
