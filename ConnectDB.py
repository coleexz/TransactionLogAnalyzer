import pyodbc

def conectar_sqlserver(servidor, puerto, usuario=None, contrasena=None, base_datos=None):
    """
    Conecta a SQL Server utilizando autenticación de SQL Server o autenticación de Windows.

    Args:
        servidor (str): Dirección del servidor.
        puerto (str): Puerto del servidor.
        base_datos (str): Nombre de la base de datos.
        usuario (str, opcional): Usuario para autenticación de SQL Server. Si no se proporciona, se utiliza autenticación de Windows.
        contrasena (str, opcional): Contraseña para autenticación de SQL Server.

    Returns:
        pyodbc.Connection o None: Objeto de conexión o None si falla.
    """
    if usuario and contrasena:
        # Autenticación de SQL Server
        conexion_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={servidor},{puerto};DATABASE={base_datos};UID={usuario};PWD={contrasena}"
    else:
        # Autenticación de Windows
        conexion_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={servidor},{puerto};DATABASE={base_datos};Trusted_Connection=yes"

    try:
        conexion = pyodbc.connect(conexion_str)
        print("Conexión exitosa")
        return conexion
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

# Ejemplo de uso para autenticación de SQL Server
conexion_sql = conectar_sqlserver('localhost', '1433', usuario='sa', contrasena='Pototo2005504', base_datos='EmpleadosDB')
# Ejemplo de uso para autenticación de Windows
#conexion_windows = conectar_sqlserver('localhost', '1433', 'MiBaseDeDatos')
