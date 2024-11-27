import pyodbc # type: ignore


def conectar_sqlserver(servidor, puerto, usuario, contrasena, base_datos):
    conexion_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={servidor},{puerto};DATABASE={base_datos};UID={usuario};PWD={contrasena}"
    try:
        conexion = pyodbc.connect(conexion_str)
        print("Conexión exitosa")
        return conexion
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None



def conexion_windows(servidor, puerto, base_datos):
    """
    Conecta a SQL Server usando autenticación de Windows.
    """
    conexion_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={servidor},{puerto};DATABASE={base_datos};Trusted_Connection=yes"
    try:
        conexion = pyodbc.connect(conexion_str)
        print("Conexión exitosa con autenticación de Windows")
        return conexion
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None


conexion_win = conexion_windows("localhost", "1433", "master")
if conexion_win:
    print("Conexión Windows establecida")