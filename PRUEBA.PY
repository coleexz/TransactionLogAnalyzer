def prueba_simulada_debug():
    # Datos de prueba
    hex_data = "0x300011000100000001C0FB390000000000050000030022002700320044616E69656C4F7274697A436F6F7264696E61646F72"
    binary_data = bytes.fromhex(hex_data[2:])

    esquema_tabla = [
        ('ID', 'int', None, 10, 0),
        ('Nombre', 'varchar', 50, None, None),
        ('Apellido', 'varchar', 50, None, None),
        ('Puesto', 'varchar', 50, None, None),
        ('Salario', 'decimal', None, 10, 2),
    ]

    expected_result = {
        'ID': 1,
        'Salario': 38000.0,
        'Nombre': 'Daniel',
        'Apellido': 'Ortiz',
        'Puesto': 'Coordinador'
    }

    # Paso 1: Leer valores fijos
    fixed_data_start = 4
    decoded_columns = {}

    decoded_columns['ID'] = int.from_bytes(binary_data[fixed_data_start:fixed_data_start + 4], "little")
    fixed_data_start += 4

    salario_precision = 10
    salario_scale = 2
    bytes_for_value = 9
    raw_value = binary_data[fixed_data_start + 1:fixed_data_start + bytes_for_value]
    is_negative = binary_data[fixed_data_start] & 0x80 != 0
    salario_value = int.from_bytes(raw_value, byteorder="little")
    if is_negative:
        salario_value = -salario_value
    decoded_columns['Salario'] = salario_value / (10 ** salario_scale)
    fixed_data_start += bytes_for_value

    print(f"DEBUG: Offset inicial: {fixed_data_start}")

    # Paso 2: Calcular mapa de bits y columnas variables
    column_offset = fixed_data_start
    total_columns = int.from_bytes(binary_data[column_offset:column_offset + 2], "little")
    print(f"DEBUG: Total columnas: {total_columns}")

    null_bitmap_size = (total_columns + 7) // 8
    variable_column_count_offset = column_offset + 2 + null_bitmap_size

    # Paso 3: Leer número de columnas variables
    variable_column_count = int.from_bytes(
        binary_data[variable_column_count_offset:variable_column_count_offset + 2], "little"
    )
    print(f"DEBUG: Variable column count: {variable_column_count}")

    offset_start = variable_column_count_offset + 2 + (variable_column_count * 2)
    print(f"DEBUG: Offset start: {offset_start}")

    # Inspeccionar la región de datos de offsets relativos
    raw_offsets_data = binary_data[offset_start:offset_start + (variable_column_count * 2)]
    print(f"DEBUG: Raw offsets data: {raw_offsets_data}")

    relative_offsets = []
    for i in range(variable_column_count):
        offset_bytes = binary_data[offset_start + i * 2:offset_start + (i + 1) * 2]
        offset = int.from_bytes(offset_bytes, "little")
        if offset < len(binary_data):  # Validar rango lógico
            relative_offsets.append(offset)
        else:
            print(f"WARNING: Offset relativo fuera de rango: {offset}")
            relative_offsets.append(len(binary_data))  # Ajustar al final del binario

    print(f"DEBUG: Offsets relativos leídos: {relative_offsets}")

    # Paso 5: Calcular base_offset y offsets absolutos
    base_offset = offset_start + variable_column_count * 2
    print(f"DEBUG: Base offset calculado: {base_offset}")

    absolute_offsets = [base_offset + rel_offset for rel_offset in relative_offsets]
    absolute_offsets.append(len(binary_data))  # Agregar final del binario
    print(f"DEBUG: Offsets absolutos ajustados: {absolute_offsets}")

    # Paso 6: Decodificar columnas variables
    variable_columns = esquema_tabla[1:4]  # Solo columnas de longitud variable
    for idx, col in enumerate(variable_columns):
        col_name, col_type, _, _, _ = col
        start = absolute_offsets[idx]
        end = absolute_offsets[idx + 1]

        if start >= len(binary_data) or start >= end:
            print(f"ERROR: Rango inválido para '{col_name}': start={start}, end={end}")
            decoded_columns[col_name] = None
            continue

        raw_data = binary_data[start:end]
        print(f"DEBUG: Datos crudos para '{col_name}': {raw_data}")

        try:
            if col_type.lower() == "nvarchar":
                decoded_value = raw_data.decode("utf-16-le").strip()
            elif col_type.lower() == "varchar":
                decoded_value = raw_data.decode("latin-1").strip()
            else:
                decoded_value = raw_data.decode("latin-1", errors="replace").strip()
        except Exception as e:
            print(f"ERROR: Fallo al decodificar '{col_type}' para la columna '{col_name}': {e}")
            decoded_value = None

        decoded_columns[col_name] = decoded_value

        # Validar contra el resultado esperado
        if decoded_columns[col_name] != expected_result[col_name]:
            print(f"ERROR: {col_name} no coincide. Decodificado: {decoded_columns[col_name]}, Esperado: {expected_result[col_name]}")
            return

    print("Resultado decodificado:", decoded_columns)
    print("¿Correcto?:", decoded_columns == expected_result)


prueba_simulada_debug()
