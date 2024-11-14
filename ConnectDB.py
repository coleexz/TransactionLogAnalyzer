import pyodbc

def conectar_sqlserver(servidor, puerto, usuario, contrasena, base_datos):
    conexion_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={servidor},{puerto};DATABASE={base_datos};UID={usuario};PWD={contrasena}"
    try:
        conexion = pyodbc.connect(conexion_str)
        print("Conexión exitosa")
        return conexion
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None
