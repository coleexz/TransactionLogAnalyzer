import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from ConnectDB import conectar_sqlserver
from tkcalendar import DateEntry
from datetime import datetime




class ReloadOnChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(".py"):
            print("Archivo modificado:", event.src_path)
            print("Recargando la aplicación...")
            os.execv(sys.executable, ['python'] + sys.argv)

def configurar_estilos():
    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TFrame", background="#FFFFFF")
    style.configure("TLabel", background="#FFFFFF", foreground="#333333")
    style.configure("TCheckbutton", background="#FFFFFF", foreground="#333333")
    style.configure("TRadiobutton", background="#FFFFFF", foreground="#333333")
    style.configure("TButton", background="#3498DB", foreground="#FFFFFF")
    style.map("TButton", background=[("active", "#2980B9")])
    style.configure("Treeview", background="#F5F5F5", foreground="#333333", rowheight=25, fieldbackground="#F5F5F5")
    style.configure("Treeview.Heading", background="#1ABC9C", foreground="#FFFFFF", font=("Helvetica", 10, "bold"))

def limpiar_interfaz():
    for widget in ventana.winfo_children():
        widget.destroy()

def mostrar_interfaz_conexion():
    limpiar_interfaz()
    ventana.geometry("550x400")
    ventana.configure(bg="#FFFFFF")
    ventana.title("Conexión a SQL Server")

    ttk.Label(ventana, text="Server:").grid(row=0, column=0, sticky="w", padx=10, pady=5)
    global entry_server, entry_port, entry_user, entry_password
    entry_server = ttk.Entry(ventana, width=30)
    entry_server.insert(0, "localhost")
    entry_server.grid(row=0, column=1, padx=10, pady=5)

    ttk.Label(ventana, text="Port:").grid(row=1, column=0, sticky="w", padx=10, pady=5)
    entry_port = ttk.Entry(ventana, width=30)
    entry_port.insert(0, "1433")
    entry_port.grid(row=1, column=1, padx=10, pady=5)

    ttk.Label(ventana, text="User:").grid(row=2, column=0, sticky="w", padx=10, pady=5)
    entry_user = ttk.Entry(ventana, width=30)
    entry_user.insert(0, "sa")
    entry_user.grid(row=2, column=1, padx=10, pady=5)

    ttk.Label(ventana, text="Password:").grid(row=3, column=0, sticky="w", padx=10, pady=5)
    entry_password = ttk.Entry(ventana, show="*", width=30)
    entry_password.grid(row=3, column=1, padx=10, pady=5)
    entry_password.insert(0, "Pototo2005504")

    btn_conectar = ttk.Button(ventana, text="Conectarse", command=conectar)
    btn_conectar.grid(row=4, column=0, columnspan=2, pady=20)

def mostrar_interfaz_popup_bases(bases_datos, conexion):
    limpiar_interfaz()
    ventana.geometry("400x150")
    ventana.title("Seleccionar base de datos")

    ttk.Label(ventana, text="Database:").grid(row=0, column=0, sticky="w", padx=10, pady=5)
    combo_database = ttk.Combobox(ventana, values=bases_datos, width=28)
    combo_database.grid(row=0, column=1, padx=10, pady=5)
    combo_database.current(0)

    def confirmar_seleccion():
        base_datos_seleccionada = combo_database.get()
        print(f"Base de datos seleccionada: {base_datos_seleccionada}")

        cursor = conexion.cursor()
        cursor.execute(f"USE {base_datos_seleccionada}")

        mostrar_interfaz_transacciones(conexion)

    btn_confirmar = ttk.Button(ventana, text="Confirmar", command=confirmar_seleccion)
    btn_confirmar.grid(row=1, column=0, columnspan=2, pady=20)



def mostrar_interfaz_transacciones(conexion):
    def validar_fecha(fecha):
        try:
            datetime.strptime(fecha, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def aplicar_filtros():
        fecha_inicio = entry_from.get()
        fecha_fin = entry_to.get()

        if not validar_fecha(fecha_inicio):
            messagebox.showerror("Error", "La fecha 'From' no es válida. Usa el formato YYYY-MM-DD.")
            return
        if not validar_fecha(fecha_fin):
            messagebox.showerror("Error", "La fecha 'To' no es válida. Usa el formato YYYY-MM-DD.")
            return

        actualizar_transacciones(
            conexion,
            var_insert.get(),
            var_update.get(),
            var_delete.get(),
            log_type_var.get(),
            tree,
            fecha_inicio,
            fecha_fin,
        )

    limpiar_interfaz()
    ventana.geometry("1620x1000")
    ventana.title("Listado de Operaciones del Log")

    filtros_frame = ttk.Frame(ventana)
    filtros_frame.grid(row=0, column=0, padx=10, pady=10, sticky="w")

    # Variables para los filtros de operación
    var_insert = tk.BooleanVar(value=True)
    var_update = tk.BooleanVar(value=True)
    var_delete = tk.BooleanVar(value=True)
    log_type_var = tk.StringVar(value="online")

    # Checkbox para Insert, Update, Delete
    ttk.Checkbutton(filtros_frame, text="Insert", variable=var_insert).grid(row=0, column=0, padx=5)
    ttk.Checkbutton(filtros_frame, text="Update", variable=var_update).grid(row=0, column=1, padx=5)
    ttk.Checkbutton(filtros_frame, text="Delete", variable=var_delete).grid(row=0, column=2, padx=5)

    # Radio buttons para seleccionar el tipo de log
    ttk.Radiobutton(filtros_frame, text="Online transaction log", variable=log_type_var, value="online").grid(row=0, column=3, padx=5)
    ttk.Radiobutton(filtros_frame, text="Backup file", variable=log_type_var, value="backup").grid(row=0, column=4, padx=5)

    # Widgets para "From" y "To" (rangos de fechas) con Entry
    ttk.Label(filtros_frame, text="From (YYYY-MM-DD):").grid(row=0, column=5, padx=5)
    entry_from = ttk.Entry(filtros_frame, width=12)
    entry_from.insert(0, datetime.now().strftime("%Y-%m-%d"))
    entry_from.grid(row=0, column=6, padx=5)

    ttk.Label(filtros_frame, text="To (YYYY-MM-DD):").grid(row=0, column=7, padx=5)
    entry_to = ttk.Entry(filtros_frame, width=12)
    entry_to.insert(0, datetime.now().strftime("%Y-%m-%d"))
    entry_to.grid(row=0, column=8, padx=5)

    # Botón Apply y Desconectar
    btn_apply = ttk.Button(filtros_frame, text="Apply", command=aplicar_filtros)
    btn_apply.grid(row=0, column=9, padx=5)

    btn_desconectar = ttk.Button(filtros_frame, text="Desconectar", command=mostrar_interfaz_conexion)
    btn_desconectar.grid(row=0, column=10, padx=5)

    # Tabla para mostrar resultados
    columnas = [" ", "Operation", "Schema", "Object", "User", "Begin time", "End time", "Transaction ID", "LSN"]
    global tree
    tree = ttk.Treeview(ventana, columns=columnas, show="headings", height=28)
    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, width=159)
    tree.grid(row=1, column=0, padx=2, pady=2)

    tree.bind("<Double-1>", lambda event: mostrar_detalle_transaccion(conexion, tree.item(tree.selection())["values"]))

def actualizar_transacciones(conexion, show_insert, show_update, show_delete, log_type, tree, date_from, date_to, backup_path=None):
    # Limpiar la tabla de resultados
    for item in tree.get_children():
        tree.delete(item)

    # Construir la lista de operaciones seleccionadas
    operations = []
    if show_insert:
        operations.append('LOP_INSERT_ROWS')
    if show_update:
        operations.append('LOP_MODIFY_ROW')
    if show_delete:
        operations.append('LOP_DELETE_ROWS')

    if not operations:
        messagebox.showwarning("Advertencia", "Seleccione al menos una operación.")
        return

    # Validar y formatear las fechas
    try:
        date_from_formatted = datetime.strptime(date_from, "%Y-%m-%d").strftime("%Y/%m/%d 00:00:00")
        date_to_formatted = datetime.strptime(date_to, "%Y-%m-%d").strftime("%Y/%m/%d 23:59:59")
    except ValueError as e:
        messagebox.showerror("Error", f"Formato de fecha inválido. Use el formato YYYY-MM-DD.\n{e}")
        return

    consulta = ""

    if log_type == "online":
        try:
            # Cambiar al contexto de la base de datos EmpleadosDB
            cursor = conexion.cursor()
            cursor.execute("USE EmpleadosDB;")

            # Generar la consulta para logs online
            consulta = f"""
            SELECT
                d.Operation AS "Operation",
                COALESCE(s.name, 'dbo') AS "Schema",
                COALESCE(d.AllocUnitName, 'Unknown') AS "Object",
                COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User",
                begin_xact.[Begin Time] AS "Begin Time",
                commit_xact.[End Time] AS "End Time",
                d.[Transaction ID] AS "Transaction ID",
                d.[Current LSN] AS "LSN"
            FROM
                fn_dblog(NULL, NULL) AS d
            LEFT JOIN
                fn_dblog(NULL, NULL) AS begin_xact
                ON d.[Transaction ID] = begin_xact.[Transaction ID] AND begin_xact.Operation = 'LOP_BEGIN_XACT'
            LEFT JOIN
                fn_dblog(NULL, NULL) AS commit_xact
                ON d.[Transaction ID] = commit_xact.[Transaction ID] AND commit_xact.Operation = 'LOP_COMMIT_XACT'
            LEFT JOIN
                sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
            LEFT JOIN
                sys.partitions p ON au.container_id = p.partition_id
            LEFT JOIN
                sys.objects o ON p.object_id = o.object_id
            LEFT JOIN
                sys.schemas s ON o.schema_id = s.schema_id
            WHERE
                d.Operation IN ({", ".join(f"'{op}'" for op in operations)})
                AND s.name IS NOT NULL
                AND s.name != 'sys'
                AND d.AllocUnitName IS NOT NULL
                AND begin_xact.[Begin Time] >= '{date_from_formatted}'
                AND begin_xact.[Begin Time] <= '{date_to_formatted}'
            ORDER BY
                d.[Current LSN];
            """
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los logs online:\n{e}")

    elif log_type == "backup":
        backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            # Habilitar autocommit para el proceso de restore
            conexion.autocommit = True
            cursor = conexion.cursor()

            # Cambiar al contexto de la base de datos master
            cursor.execute("USE master;")

            # Restaurar el transaction log en EmpleadosBackup
            cursor.execute(f"""
                RESTORE LOG EmpleadosBackup
                FROM DISK = '{backup_path}'
                WITH NORECOVERY;
            """)

            # Recuperar EmpleadosBackup después del restore del log
            cursor.execute("RESTORE DATABASE EmpleadosBackup WITH RECOVERY;")

            messagebox.showinfo("Éxito", "El transaction log ha sido restaurado correctamente en 'EmpleadosBackup'.")

            # Generar la consulta para logs en la base restaurada
            consulta = f"""
            USE EmpleadosBackup;
            SELECT
                d.Operation AS "Operation",
                COALESCE(s.name, 'dbo') AS "Schema",
                COALESCE(d.AllocUnitName, 'Unknown') AS "Object",
                COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User",
                begin_xact.[Begin Time] AS "Begin Time",
                commit_xact.[End Time] AS "End Time",
                d.[Transaction ID] AS "Transaction ID",
                d.[Current LSN] AS "LSN"
            FROM
                fn_dblog(NULL, NULL) AS d
            LEFT JOIN
                fn_dblog(NULL, NULL) AS begin_xact
                ON d.[Transaction ID] = begin_xact.[Transaction ID] AND begin_xact.Operation = 'LOP_BEGIN_XACT'
            LEFT JOIN
                fn_dblog(NULL, NULL) AS commit_xact
                ON d.[Transaction ID] = commit_xact.[Transaction ID] AND commit_xact.Operation = 'LOP_COMMIT_XACT'
            LEFT JOIN
                sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
            LEFT JOIN
                sys.partitions p ON au.container_id = p.partition_id
            LEFT JOIN
                sys.objects o ON p.object_id = o.object_id
            LEFT JOIN
                sys.schemas s ON o.schema_id = s.schema_id
            WHERE
                d.Operation IN ({", ".join(f"'{op}'" for op in operations)})
                AND s.name IS NOT NULL
                AND s.name != 'sys'
                AND d.AllocUnitName IS NOT NULL
                AND begin_xact.[Begin Time] >= '{date_from_formatted}'
                AND begin_xact.[Begin Time] <= '{date_to_formatted}'
            ORDER BY
                d.[Current LSN];
            """
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo restaurar el transaction log desde el archivo de backup:\n{e}")
        finally:
            conexion.autocommit = False

    # Ejecutar la consulta y cargar los resultados
    try:
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            messagebox.showinfo("Información", "No se encontraron transacciones con los filtros seleccionados.")
        else:
            for index, row in enumerate(resultados, start=1):
                clean_row = [index]
                for item in row:

                    if isinstance(item, str) and "." in item:
                        parts = item.split(".")
                        if len(parts) > 1:
                            clean_row.append(parts[1])
                        else:
                            clean_row.append(item)
                    else:
                        clean_row.append(item)
                tree.insert("", "end", values=clean_row)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudieron cargar las transacciones:\n{e}")


def mostrar_detalle_transaccion(conexion, detalle_transaccion):
    ventana_detalle = tk.Toplevel(ventana)
    ventana_detalle.title("Detalle de la Operación")
    ventana_detalle.geometry("800x600")

    transaction_id = detalle_transaccion[7]

    # Crear el notebook para las pestañas
    notebook = ttk.Notebook(ventana_detalle)
    notebook.pack(fill="both", expand=True)

    # Pestañas vacías o preparadas
    frame_detalles = ttk.Frame(notebook)
    notebook.add(frame_detalles, text="Operation details")
    definir_contenido_operation_details(frame_detalles, conexion, transaction_id)

    frame_historial = ttk.Frame(notebook)
    notebook.add(frame_historial, text="Row history")
    definir_contenido_row_history(frame_historial, conexion, transaction_id)


    frame_undo = ttk.Frame(notebook)
    notebook.add(frame_undo, text="Undo script")
    definir_contenido_undo_script(frame_undo, conexion, transaction_id)

    frame_redo = ttk.Frame(notebook)
    notebook.add(frame_redo, text="Redo script")
    definir_contenido_redo_script(frame_redo, conexion, transaction_id)

    # Pestaña para la información de la transacción
    operation = detalle_transaccion[1]
    frame_informacion = ttk.Frame(notebook)
    notebook.add(frame_informacion, text="Transaction information")
    definir_contenido_transaction_info(frame_informacion, conexion, transaction_id, operation)


def definir_contenido_operation_details(frame, conexion, transaction_id):
    """Define el contenido de la pestaña Operation details con datos reales obtenidos de la base de datos."""
    # Limpiar el frame antes de agregar los nuevos widgets
    for widget in frame.winfo_children():
        widget.destroy()

    # Consulta SQL para obtener los detalles de la operación con esquema, tabla y columna
    consulta = f"""
    SELECT
        CONCAT(s.name, '.', o.name, '.', c.name) AS "Field", -- esquema.tabla.columna
        t.name AS "Type",
        d.[RowLog Contents 0] AS "Old Value",
        d.[RowLog Contents 1] AS "New Value"
    FROM
        fn_dblog(NULL, NULL) d
    JOIN
        sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
    JOIN
        sys.partitions p ON au.container_id = p.partition_id
    JOIN
        sys.objects o ON p.object_id = o.object_id
    JOIN
        sys.schemas s ON o.schema_id = s.schema_id
    JOIN
        sys.columns c ON c.object_id = o.object_id
    JOIN
        sys.types t ON c.user_type_id = t.user_type_id
    WHERE
        d.[Transaction ID] = '{transaction_id}'
        AND d.Operation IN ('LOP_MODIFY_ROW', 'LOP_INSERT_ROWS', 'LOP_DELETE_ROWS')
    ORDER BY
        d.[Current LSN];
    """

    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            label = ttk.Label(frame, text="No se encontraron detalles para esta operación.")
            label.pack()
            return

        # Crear tabla para mostrar los detalles
        columnas = ["Field", "Type", "Old Value", "New Value"]
        tree = ttk.Treeview(frame, columns=columnas, show="headings", height=8)
        tree.pack(fill="both", expand=True)

        for col in columnas:
            tree.heading(col, text=col)
            tree.column(col, anchor="w", width=200)

        # Insertar los datos reales en la tabla
        for row in resultados:
            tree.insert("", "end", values=row)

    except Exception as e:
        label = ttk.Label(frame, text=f"Error al obtener los detalles de la operación: {e}")
        label.pack()


def definir_contenido_row_history(frame, conexion, transaction_id):
    """Define el contenido de la pestaña Row history en formato de tabla"""

    # Limpiar el frame para agregar nuevos widgets
    for widget in frame.winfo_children():
        widget.destroy()

    # Crear tabla para mostrar el historial
    columnas = ["Operation", "Date", "User Name", "LSN", "id", "Name", "Description", "Column8", "Column9", "Column10"]
    tree = ttk.Treeview(frame, columns=columnas, show="headings", height=10)
    tree.pack(fill="both", expand=True)

    # Configurar encabezados y ancho de columnas
    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, anchor="w", width=150)

    # Ejecutar el query para obtener los datos
    consulta = f"""
    SELECT
        d.Operation AS "Operation",
        begin_xact.[Begin Time] AS "Date",
        COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User Name",
        d.[Current LSN] AS "LSN",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 0]) AS "id",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 1]) AS "Name",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 2]) AS "Description",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 3]) AS "Column8",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 4]) AS "Column9",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 5]) AS "Column10"
    FROM
        fn_dblog(NULL, NULL) d
    LEFT JOIN
        fn_dblog(NULL, NULL) AS begin_xact
        ON d.[Transaction ID] = begin_xact.[Transaction ID]
        AND begin_xact.Operation = 'LOP_BEGIN_XACT'
    WHERE
        d.[Transaction ID] = '{transaction_id}' -- Usar el Transaction ID seleccionado
        AND d.AllocUnitName NOT LIKE 'sys%'
        AND d.AllocUnitName != 'Unknown Alloc Unit'
        AND d.Operation IN ('LOP_INSERT_ROWS', 'LOP_MODIFY_ROW', 'LOP_DELETE_ROWS')
    ORDER BY
        d.[Current LSN];
    """

    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            # Mostrar mensaje si no hay datos
            label = ttk.Label(frame, text="No se encontró historial de fila para esta transacción.")
            label.pack()
        else:
            # Insertar filas en la tabla
            for row in resultados:
                tree.insert("", "end", values=row)

    except Exception as e:
        # Manejar errores
        label = ttk.Label(frame, text=f"Error al obtener el historial de fila: {e}")
        label.pack()



def definir_contenido_undo_script(frame, conexion, transaction_id):
    """Genera y muestra el script de deshacer para la transacción seleccionada"""
    # Limpiar el frame para agregar nuevos widgets
    for widget in frame.winfo_children():
        widget.destroy()

    # Etiqueta inicial
    label = ttk.Label(frame, text="Script de deshacer", font=("Arial", 12, "bold"))
    label.pack(anchor="w", padx=10, pady=5)

    # Consulta para obtener los datos de la transacción
    consulta = f"""
    SELECT
        CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
        d.Operation AS "Operation",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 0]) AS "Old Value",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 1]) AS "New Value",
        o.name AS "Table",
        s.name AS "Schema"
    FROM
        fn_dblog(NULL, NULL) d
    JOIN
        sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
    JOIN
        sys.partitions p ON au.container_id = p.partition_id
    JOIN
        sys.objects o ON p.object_id = o.object_id
    JOIN
        sys.schemas s ON o.schema_id = s.schema_id
    JOIN
        sys.columns c ON o.object_id = c.object_id
    WHERE
        d.[Transaction ID] = '{transaction_id}'
        AND d.Operation IN ('LOP_MODIFY_ROW', 'LOP_INSERT_ROWS', 'LOP_DELETE_ROWS')
    ORDER BY
        d.[Current LSN];
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            ttk.Label(frame, text="No se encontraron datos para generar el script.").pack(anchor="w", padx=10, pady=5)
            return

        # Generar el script de deshacer
        undo_script = "BEGIN TRANSACTION;\n"
        for row in resultados:
            field, operation, old_value, new_value, table, schema = row
            if operation == "LOP_MODIFY_ROW":
                undo_script += f"UPDATE [{schema}].[{table}] SET [{field.split('.')[-1]}] = '{old_value}' WHERE [{field.split('.')[-1]}] = '{new_value}';\n"
            elif operation == "LOP_INSERT_ROWS":
                undo_script += f"DELETE FROM [{schema}].[{table}] WHERE [{field.split('.')[-1]}] = '{new_value}';\n"
            elif operation == "LOP_DELETE_ROWS":
                undo_script += f"INSERT INTO [{schema}].[{table}] ([{field.split('.')[-1]}]) VALUES ('{old_value}');\n"
        undo_script += "COMMIT TRANSACTION;\n"

        # Mostrar el script en un Text widget
        text_widget = tk.Text(frame, wrap="none", height=20, width=80, font=("Fira Code", 16), bg="white", fg="black")
        text_widget.insert("1.0", undo_script)
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)

        # Deshabilitar edición
        text_widget.configure(state="disabled")

    except Exception as e:
        ttk.Label(frame, text=f"Error al generar el script: {e}").pack(anchor="w", padx=10, pady=5)


def definir_contenido_redo_script(frame, conexion, transaction_id):
    """Genera y muestra el script de rehacer para la transacción seleccionada"""
    # Limpiar el frame para agregar nuevos widgets
    for widget in frame.winfo_children():
        widget.destroy()

    # Etiqueta inicial
    label = ttk.Label(frame, text="Script de rehacer", font=("Arial", 12, "bold"))
    label.pack(anchor="w", padx=10, pady=5)

    # Consulta para obtener los datos de la transacción
    consulta = f"""
    SELECT
        CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
        d.Operation AS "Operation",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 0]) AS "Old Value",
        CONVERT(VARCHAR(MAX), d.[RowLog Contents 1]) AS "New Value",
        o.name AS "Table",
        s.name AS "Schema"
    FROM
        fn_dblog(NULL, NULL) d
    JOIN
        sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
    JOIN
        sys.partitions p ON au.container_id = p.partition_id
    JOIN
        sys.objects o ON p.object_id = o.object_id
    JOIN
        sys.schemas s ON o.schema_id = s.schema_id
    JOIN
        sys.columns c ON o.object_id = c.object_id
    WHERE
        d.[Transaction ID] = '{transaction_id}'
        AND d.Operation IN ('LOP_MODIFY_ROW', 'LOP_INSERT_ROWS', 'LOP_DELETE_ROWS')
    ORDER BY
        d.[Current LSN];
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            ttk.Label(frame, text="No se encontraron datos para generar el script.").pack(anchor="w", padx=10, pady=5)
            return

        # Generar el script de rehacer
        redo_script = "BEGIN TRANSACTION;\n"
        for row in resultados:
            field, operation, old_value, new_value, table, schema = row
            if operation == "LOP_MODIFY_ROW":
                redo_script += f"UPDATE [{schema}].[{table}] SET [{field.split('.')[-1]}] = '{new_value}' WHERE [{field.split('.')[-1]}] = '{old_value}';\n"
            elif operation == "LOP_INSERT_ROWS":
                redo_script += f"INSERT INTO [{schema}].[{table}] ([{field.split('.')[-1]}]) VALUES ('{new_value}');\n"
            elif operation == "LOP_DELETE_ROWS":
                redo_script += f"DELETE FROM [{schema}].[{table}] WHERE [{field.split('.')[-1]}] = '{old_value}';\n"
        redo_script += "COMMIT TRANSACTION;\n"

        # Mostrar el script en un Text widget
        text_widget = tk.Text(frame, wrap="none", height=20, width=80, font=("Fira Code", 16), bg="white", fg="black")
        text_widget.insert("1.0", redo_script)
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)

        # Deshabilitar edición
        text_widget.configure(state="disabled")

    except Exception as e:
        ttk.Label(frame, text=f"Error al generar el script: {e}").pack(anchor="w", padx=10, pady=5)



def definir_contenido_transaction_info(frame, conexion, transaction_id, operation):
    """Define el contenido de la pestaña Transaction Information"""
    # Limpiar el frame para agregar nuevos widgets
    for widget in frame.winfo_children():
        widget.destroy()

    # Validar si el Transaction ID existe
    if not transaction_id:
        label = ttk.Label(frame, text="No se pudo encontrar el Transaction ID.")
        label.pack()
        return

    # Consulta para obtener los detalles de la operación específica seleccionada
    consulta = f"""
    SELECT
        begin_xact.[Begin Time] AS "Begin Time",
        CASE
            WHEN commit_xact.Operation = 'LOP_COMMIT_XACT' THEN 'Committed'
            WHEN commit_xact.Operation = 'LOP_ABORT_XACT' THEN 'Aborted'
            ELSE 'In Progress'
        END AS "State",
        d.Operation AS "Operation",
        COALESCE(s.name, 'Unknown') AS "Schema",
        COALESCE(d.AllocUnitName, 'Unknown') AS "Object",
        'N/A' AS "Parent Schema",
        'N/A' AS "Parent Object",
        COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User",
        CASE
            WHEN d.[Transaction SID] IS NOT NULL THEN '0x' + CONVERT(VARCHAR(MAX), d.[Transaction SID], 1)
            ELSE '0x0x01'
        END AS "User ID"
    FROM
        fn_dblog(NULL, NULL) AS d
    LEFT JOIN
        fn_dblog(NULL, NULL) AS begin_xact
        ON d.[Transaction ID] = begin_xact.[Transaction ID] AND begin_xact.Operation = 'LOP_BEGIN_XACT'
    LEFT JOIN
        fn_dblog(NULL, NULL) AS commit_xact
        ON d.[Transaction ID] = commit_xact.[Transaction ID] AND commit_xact.Operation IN ('LOP_COMMIT_XACT', 'LOP_ABORT_XACT')
    LEFT JOIN
        sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
    LEFT JOIN
        sys.partitions p ON au.container_id = p.partition_id
    LEFT JOIN
        sys.objects o ON p.object_id = o.object_id
    LEFT JOIN
        sys.schemas s ON o.schema_id = s.schema_id
    WHERE
        d.[Transaction ID] = '{transaction_id}' AND d.Operation = '{operation}'
    ORDER BY
        d.[Current LSN];
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultado = cursor.fetchone()
        if not resultado:
            label = ttk.Label(frame, text="No se encontró información para esta operación.")
            label.pack()
            return

        #quiero que la columna de object soolo muestre lo qeu esta despues del primer punto y antes del segundo
        resultado [4] = resultado[4].split('.')[1]

        # Crear tabla para mostrar la información
        columnas = ["Field", "Value"]
        tree = ttk.Treeview(frame, columns=columnas, show="headings", height=10)
        tree.pack(fill="both", expand=True)

        for col in columnas:
            tree.heading(col, text=col)
            tree.column(col, anchor="w", width=200)

        campos = ["Begin Time", "State", "Operation", "Schema", "Object", "Parent Schema", "Parent Object", "User", "User ID"]
        for idx, valor in enumerate(resultado):
            tree.insert("", "end", values=(campos[idx], valor))

    except Exception as e:
        label = ttk.Label(frame, text=f"Error al obtener los detalles: {e}")
        label.pack()




#=======================================================================================================


def conectar():
    servidor = entry_server.get()
    puerto = entry_port.get()
    usuario = entry_user.get()
    contrasena = entry_password.get()
    conexion = conectar_sqlserver(servidor, puerto, usuario, contrasena, "master")

    if conexion:
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sys.databases;")
        bases_datos = [row[0] for row in cursor.fetchall()]
        mostrar_interfaz_popup_bases(bases_datos, conexion)
    else:
        messagebox.showerror("Error", "No se pudo establecer la conexión")




def start_watchdog():
    observer = Observer()
    event_handler = ReloadOnChangeHandler()
    observer.schedule(event_handler, path=".", recursive=True)
    observer.start()

    try:
        configurar_estilos()
        mostrar_interfaz_conexion()
        ventana.mainloop()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

ventana = tk.Tk()

if __name__ == "__main__":
    start_watchdog()
