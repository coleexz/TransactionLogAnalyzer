def decode_rowlog(hex_data):
    try:
        binary_data = bytes.fromhex(hex_data)

        status_bits = binary_data[:2]
        print(f"Status Bits: {status_bits.hex()}")

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

        var_columns = []
        for i in range(len(var_offsets)):
            start = var_offsets[i - 1] if i > 0 else column_offset + 2 + null_bitmap_size + 2 + len(var_offsets) * 2
            end = var_offsets[i]
            if start > len(binary_data) or end > len(binary_data):
                print("Error: Offsets fuera de los límites de los datos.")
                return
            var_columns.append(binary_data[start:end].decode('utf-8', errors='replace').strip())

        if var_offsets:
            start = var_offsets[-1]
            if start < len(binary_data):
                var_columns.append(binary_data[start:].decode('utf-8', errors='replace').strip())
        print(f"Columnas de Longitud Variable: {var_columns}")

    except Exception as e:
        print(f"Error al procesar RowLog Contents: {e}")

# Ejemplo
rowlog_hex = "300011001D00000001604D2F0000000000050000030025002A003A0053656261737469E16E4EFAF1657A4167656E74652064652056656E746173"
decode_rowlog(rowlog_hex)
