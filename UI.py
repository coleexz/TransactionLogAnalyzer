import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from ConnectDB import conectar_sqlserver
from tkcalendar import DateEntry
from datetime import datetime
from DecodeRowLog import decode_rowlog



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

    var_insert = tk.BooleanVar(value=True)
    var_update = tk.BooleanVar(value=True)
    var_delete = tk.BooleanVar(value=True)
    log_type_var = tk.StringVar(value="online")

    ttk.Checkbutton(filtros_frame, text="Insert", variable=var_insert).grid(row=0, column=0, padx=5)
    ttk.Checkbutton(filtros_frame, text="Update", variable=var_update).grid(row=0, column=1, padx=5)
    ttk.Checkbutton(filtros_frame, text="Delete", variable=var_delete).grid(row=0, column=2, padx=5)

    ttk.Radiobutton(filtros_frame, text="Online transaction log", variable=log_type_var, value="online").grid(row=0, column=3, padx=5)
    ttk.Radiobutton(filtros_frame, text="Backup file", variable=log_type_var, value="backup").grid(row=0, column=4, padx=5)

    ttk.Label(filtros_frame, text="From (YYYY-MM-DD):").grid(row=0, column=5, padx=5)
    entry_from = ttk.Entry(filtros_frame, width=12)
    entry_from.insert(0, datetime.now().strftime("%Y-%m-%d"))
    entry_from.grid(row=0, column=6, padx=5)

    ttk.Label(filtros_frame, text="To (YYYY-MM-DD):").grid(row=0, column=7, padx=5)
    entry_to = ttk.Entry(filtros_frame, width=12)
    entry_to.insert(0, datetime.now().strftime("%Y-%m-%d"))
    entry_to.grid(row=0, column=8, padx=5)

    btn_apply = ttk.Button(filtros_frame, text="Apply", command=aplicar_filtros)
    btn_apply.grid(row=0, column=9, padx=5)

    btn_desconectar = ttk.Button(filtros_frame, text="Desconectar", command=mostrar_interfaz_conexion)
    btn_desconectar.grid(row=0, column=10, padx=5)

    columnas = [" ", "Operation", "Schema", "Object", "User", "Begin time", "End time", "Transaction ID", "LSN"]
    global tree
    tree = ttk.Treeview(ventana, columns=columnas, show="headings", height=28)
    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, width=159)
    tree.grid(row=1, column=0, padx=2, pady=2)


    tree.bind("<Double-1>", lambda event: mostrar_detalle_transaccion(conexion, tree.item(tree.selection())["values"], log_type_var.get()))

def actualizar_transacciones(conexion, show_insert, show_update, show_delete, log_type, tree, date_from, date_to, backup_path=None):
    for item in tree.get_children():
        tree.delete(item)

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

    try:
        date_from_formatted = datetime.strptime(date_from, "%Y-%m-%d").strftime("%Y/%m/%d 00:00:00")
        date_to_formatted = datetime.strptime(date_to, "%Y-%m-%d").strftime("%Y/%m/%d 23:59:59")
    except ValueError as e:
        messagebox.showerror("Error", f"Formato de fecha inválido. Use el formato YYYY-MM-DD.\n{e}")
        return

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
        try:
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
                AND begin_xact.[Begin Time] >= '{date_from_formatted}'
                AND begin_xact.[Begin Time] <= '{date_to_formatted}'
                AND d.AllocUnitName NOT LIKE 'sys%'
                AND d.ALLocUnitName != 'Unknown Alloc Unit'
            ORDER BY
                d.[Current LSN];
            """

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
            messagebox.showerror("Error", f"No se pudieron cargar los logs online:\n{e}")

    if log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:

            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
            DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

            consulta = f"""
            SELECT
                d.Operation AS "Operation",
                COALESCE(s.name, 'dbo') AS "Schema",
                COALESCE(
                    (SELECT OBJECT_NAME(p.object_id)
                    FROM sys.allocation_units au
                    JOIN sys.partitions p ON au.container_id = p.hobt_id
                    WHERE au.allocation_unit_id = d.AllocUnitId
                    ), 'Unknown') AS "Object",
                COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User",
                begin_xact.[Begin Time] AS "Begin Time",
                commit_xact.[End Time] AS "End Time",
                d.[Transaction ID] AS "Transaction ID",
                d.[Current LSN] AS "LSN"
            FROM
                #TempTransactionLog d
            LEFT JOIN
                #TempTransactionLog AS begin_xact
                ON d.[Transaction ID] = begin_xact.[Transaction ID] AND begin_xact.Operation = 'LOP_BEGIN_XACT'
            LEFT JOIN
                #TempTransactionLog AS commit_xact
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
                AND begin_xact.[Begin Time] >= '{date_from_formatted}'
                AND begin_xact.[Begin Time] <= '{date_to_formatted}'
                AND s.name != 'sys' -- Excluir objetos del esquema sys
                AND (
                    SELECT OBJECT_NAME(p.object_id)
                    FROM sys.allocation_units au
                    JOIN sys.partitions p ON au.container_id = p.hobt_id
                    WHERE au.allocation_unit_id = d.AllocUnitId
                ) IS NOT NULL -- Excluir Unknown
            ORDER BY
                d.[Current LSN];

            """
            cursor.execute(consulta)
            resultados = cursor.fetchall()

            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

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
            messagebox.showerror("Error", f"No se pudieron cargar los logs desde el backup:\n{e}")


def mostrar_detalle_transaccion(conexion, detalle_transaccion, log_type):
    ventana_detalle = tk.Toplevel(ventana)
    ventana_detalle.title("Detalle de la Operación")
    ventana_detalle.geometry("800x600")

    transaction_id = detalle_transaccion[7]
    schema = detalle_transaccion[2]
    print('schema: ', schema)
    object_name = detalle_transaccion[3]
    print('object_name: ', object_name)

    notebook = ttk.Notebook(ventana_detalle)
    notebook.pack(fill="both", expand=True)

    frame_detalles = ttk.Frame(notebook)
    notebook.add(frame_detalles, text="Operation details")
    definir_contenido_operation_details(frame_detalles, conexion, transaction_id, log_type)

    frame_historial = ttk.Frame(notebook)
    notebook.add(frame_historial, text="Row history")
    definir_contenido_row_history(frame_historial, conexion, transaction_id, log_type, schema=schema, object_name=object_name)

    frame_undo = ttk.Frame(notebook)
    notebook.add(frame_undo, text="Undo script")
    definir_contenido_undo_script(frame_undo, conexion, transaction_id, log_type)

    frame_redo = ttk.Frame(notebook)
    notebook.add(frame_redo, text="Redo script")
    definir_contenido_redo_script(frame_redo, conexion, transaction_id, log_type)

    operation = detalle_transaccion[1]
    frame_informacion = ttk.Frame(notebook)
    notebook.add(frame_informacion, text="Transaction information")
    definir_contenido_transaction_info(frame_informacion, conexion, transaction_id, operation, log_type)


def definir_contenido_operation_details(frame, conexion, transaction_id, log_type, backup_path=None):
    """Define el contenido de la pestaña Operation details con datos reales obtenidos de la base de datos."""
    for widget in frame.winfo_children():
        widget.destroy()

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
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
    elif log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

            consulta = f"""
            SELECT
                CONCAT(s.name, '.', o.name, '.', c.name) AS "Field", -- esquema.tabla.columna
                t.name AS "Type",
                d.[RowLog Contents 0] AS "Old Value",
                d.[RowLog Contents 1] AS "New Value"
            FROM
                #TempTransactionLog d
            LEFT JOIN
                sys.allocation_units au ON d.AllocUnitId = au.allocation_unit_id
            LEFT JOIN
                sys.partitions p ON au.container_id = p.partition_id
            LEFT JOIN
                sys.objects o ON p.object_id = o.object_id
            LEFT JOIN
                sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN
                sys.columns c ON c.object_id = o.object_id
            LEFT JOIN
                sys.types t ON c.user_type_id = t.user_type_id
            WHERE
                d.[Transaction ID] = '{transaction_id}'
                AND d.Operation IN ('LOP_MODIFY_ROW', 'LOP_INSERT_ROWS', 'LOP_DELETE_ROWS')
            ORDER BY
                d.[Current LSN];
            """
            cursor.execute(consulta)
            resultados = cursor.fetchall()

            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

        except Exception as e:
            label = ttk.Label(frame, text=f"Error al cargar datos del backup:\n{e}")
            label.pack()
            return

    try:
        if log_type == "online":
            cursor.execute(consulta)
            resultados = cursor.fetchall()

        if not resultados:
            label = ttk.Label(frame, text="No se encontraron detalles para esta operación.")
            label.pack()
            return

        columnas = ["Field", "Type", "Old Value", "New Value"]
        tree = ttk.Treeview(frame, columns=columnas, show="headings", height=8)
        tree.pack(fill="both", expand=True)

        for col in columnas:
            tree.heading(col, text=col)
            tree.column(col, anchor="w", width=200)

        for row in resultados:
            # Descomponer valores esperados (ahora 4 valores)
            field, tipo, old_value_hex, new_value_hex = row

            # Decodificar los valores si existen
            old_value = decode_rowlog(conexion, field.split('.')[0], field.split('.')[1], old_value_hex) if old_value_hex else ""
            new_value = decode_rowlog(conexion, field.split('.')[0], field.split('.')[1], new_value_hex) if new_value_hex else ""

            # Obtener el valor específico para cada campo
            old_value_specific = old_value.get(field.split('.')[-1], "") if old_value else ""
            new_value_specific = new_value.get(field.split('.')[-1], "") if new_value else ""

            # Insertar en la tabla con valores decodificados específicos
            tree.insert("", "end", values=(field, tipo, old_value_specific, new_value_specific))

    except Exception as e:
        label = ttk.Label(frame, text=f"Error al obtener los detalles de la operación:\n{e}")
        label.pack()

def definir_contenido_row_history(frame, conexion, transaction_id, log_type, backup_path=None, schema= None, object_name=None):
    """Define el contenido de la pestaña Row history en formato de tabla"""

    print('schema en row history: ', schema)
    print('object_name en row history: ', object_name)

    for widget in frame.winfo_children():
        widget.destroy()

    columnas = ["Operation", "Date", "User Name", "LSN", "id", "Name", "Description", "Column8", "Column9", "Column10"]
    tree = ttk.Treeview(frame, columns=columnas, show="headings", height=10)
    tree.pack(fill="both", expand=True)

    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, anchor="w", width=150)

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
        consulta = f"""
        SELECT
            d.Operation AS "Operation",
            begin_xact.[Begin Time] AS "Date",
            COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User Name",
            d.[Current LSN] AS "LSN",
            d.[RowLog Contents 0] AS "id",
            d.[RowLog Contents 1] AS "Name",
            d.Description AS "Description",
            d.[RowLog Contents 3] AS "Column8",
            d.[RowLog Contents 4] AS "Column9",
            d.[RowLog Contents 5] AS "Column10"
        FROM
            fn_dblog(NULL, NULL) d
        LEFT JOIN
            fn_dblog(NULL, NULL) AS begin_xact
            ON d.[Transaction ID] = begin_xact.[Transaction ID]
            AND begin_xact.Operation = 'LOP_BEGIN_XACT'
        WHERE
            d.[Transaction ID] = '{transaction_id}'
            AND d.AllocUnitName NOT LIKE 'sys%'
            AND d.AllocUnitName != 'Unknown Alloc Unit'
            AND d.Operation IN ('LOP_INSERT_ROWS', 'LOP_MODIFY_ROW', 'LOP_DELETE_ROWS')
        ORDER BY
            d.[Current LSN];
        """
    elif log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

            consulta = f"""
            SELECT
                d.Operation AS "Operation",
                begin_xact.[Begin Time] AS "Date",
                COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User Name",
                d.[Current LSN] AS "LSN",
                d.[RowLog Contents 0] AS "id",
                d.[RowLog Contents 1] AS "Name",
                d.Description AS "Description",
                d.[RowLog Contents 3] AS "Column8",
                d.[RowLog Contents 4] AS "Column9",
                d.[RowLog Contents 5] AS "Column10"
            FROM
                #TempTransactionLog d
            LEFT JOIN
                #TempTransactionLog AS begin_xact
                ON d.[Transaction ID] = begin_xact.[Transaction ID]
                AND begin_xact.Operation = 'LOP_BEGIN_XACT'
            WHERE
                d.[Transaction ID] = '{transaction_id}'
                AND d.AllocUnitName NOT LIKE 'sys%'
                AND d.AllocUnitName != 'Unknown Alloc Unit'
                AND d.Operation IN ('LOP_INSERT_ROWS', 'LOP_MODIFY_ROW', 'LOP_DELETE_ROWS')
            ORDER BY
                d.[Current LSN];
            """
            cursor.execute(consulta)
            resultados = cursor.fetchall()

            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

        except Exception as e:
            label = ttk.Label(frame, text=f"Error al cargar datos del backup:\n{e}")
            label.pack()
            return

    try:
        if log_type == "online":
            cursor.execute(consulta)
            resultados = cursor.fetchall()

        if not resultados:
            label = ttk.Label(frame, text="No se encontró historial de fila para esta transacción.")
            label.pack()
        else:
            for row in resultados:
                formatted_row = [
                    row[0],  # Operation
                    row[1],  # Date
                    row[2],  # User Name
                    row[3],  # LSN
                    decode_rowlog(conexion, schema, object_name, row[4]) if row[4] else "N/A",  # id
                    decode_rowlog(conexion, schema, object_name, row[5]) if row[5] else "N/A",  # Name
                    row[6],  # Description
                    decode_rowlog(conexion, schema, object_name, row[7]) if row[7] else "N/A",  # Column8
                    decode_rowlog(conexion, schema, object_name, row[8]) if row[8] else "N/A",  # Column9
                    decode_rowlog(conexion, schema, object_name, row[9]) if row[9] else "N/A",  # Column10
                ]
                tree.insert("", "end", values=formatted_row)

    except Exception as e:
        label = ttk.Label(frame, text=f"Error al obtener el historial de fila:\n{e}")
        label.pack()

def definir_contenido_undo_script(frame, conexion, transaction_id, log_type, backup_path=None):
    """Genera y muestra el script de deshacer para la transacción seleccionada"""
    for widget in frame.winfo_children():
        widget.destroy()

    label = ttk.Label(frame, text="Script de deshacer", font=("Arial", 12, "bold"))
    label.pack(anchor="w", padx=10, pady=5)

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
        consulta = f"""
        SELECT
            CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
            d.Operation AS "Operation",
            d.[RowLog Contents 0] AS "Old Value",
            d.[RowLog Contents 1] AS "New Value",
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
    elif log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

            consulta = f"""
            SELECT
                CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
                d.Operation AS "Operation",
                d.[RowLog Contents 0] AS "Old Value",
                d.[RowLog Contents 1] AS "New Value",
                o.name AS "Table",
                s.name AS "Schema"
            FROM
                #TempTransactionLog d
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
        except Exception as e:
            ttk.Label(frame, text=f"Error al cargar datos del backup:\n{e}").pack(anchor="w", padx=10, pady=5)
            return

    try:
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if log_type == "backup":
            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

        if not resultados:
            ttk.Label(frame, text="No se encontraron datos para generar el script.").pack(anchor="w", padx=10, pady=5)
            return

        undo_script = "BEGIN TRANSACTION;\n"
        for row in resultados:
            field, operation, old_value_hex, new_value_hex, table, schema = row

            # Intentar decodificar los valores
            old_value = decode_rowlog(conexion, schema, table, old_value_hex) if old_value_hex else None
            new_value = decode_rowlog(conexion, schema, table, new_value_hex) if new_value_hex else None

            if operation == "LOP_MODIFY_ROW":
                if old_value and new_value:
                    for col_name in old_value.keys():
                        undo_script += f"UPDATE [{schema}].[{table}] SET [{col_name}] = '{old_value[col_name]}' WHERE [{col_name}] = '{new_value.get(col_name, '')}';\n"
                elif old_value or new_value:
                    undo_script += f"-- Operación MODIFICAR con valores parciales en la tabla [{schema}].[{table}]\n"

            elif operation == "LOP_INSERT_ROWS":
                if new_value:
                    conditions = ' AND '.join([f"[{col}] = '{val}'" for col, val in new_value.items()])
                    undo_script += f"DELETE FROM [{schema}].[{table}] WHERE {conditions};\n"
                elif old_value:
                    conditions = ' AND '.join([f"[{col}] = '{val}'" for col, val in old_value.items()])
                    undo_script += f"DELETE FROM [{schema}].[{table}] WHERE {conditions};\n"

            elif operation == "LOP_DELETE_ROWS":
                if old_value:
                    columns = ', '.join([f"[{col}]" for col in old_value.keys()])
                    values = ', '.join([f"'{val}'" for val in old_value.values()])
                    undo_script += f"INSERT INTO [{schema}].[{table}] ({columns}) VALUES ({values});\n"
                elif new_value:
                    columns = ', '.join([f"[{col}]" for col in new_value.keys()])
                    values = ', '.join([f"'{val}'" for val in new_value.values()])
                    undo_script += f"-- Operación DELETE con valores parciales en la tabla [{schema}].[{table}]\n"

        undo_script += "COMMIT TRANSACTION;\n"


        text_widget = tk.Text(frame, wrap="none", height=20, width=80, font=("Fira Code", 16), bg="white", fg="black")
        text_widget.insert("1.0", undo_script)
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)

        text_widget.configure(state="disabled")

    except Exception as e:
        ttk.Label(frame, text=f"Error al generar el script:\n{e}").pack(anchor="w", padx=10, pady=5)

def definir_contenido_redo_script(frame, conexion, transaction_id, log_type, backup_path=None):
    """Genera y muestra el script de rehacer para la transacción seleccionada"""
    for widget in frame.winfo_children():
        widget.destroy()

    label = ttk.Label(frame, text="Script de rehacer", font=("Arial", 12, "bold"))
    label.pack(anchor="w", padx=10, pady=5)

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
        consulta = f"""
        SELECT
            CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
            d.Operation AS "Operation",
            d.[RowLog Contents 0] AS "Old Value",
            d.[RowLog Contents 1] AS "New Value",
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
    elif log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

            consulta = f"""
            SELECT
                CONCAT(s.name, '.', o.name, '.', c.name) AS "Field",
                d.Operation AS "Operation",
                d.[RowLog Contents 0] AS "Old Value",
                d.[RowLog Contents 1] AS "New Value",
                o.name AS "Table",
                s.name AS "Schema"
            FROM
                #TempTransactionLog d
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
        except Exception as e:
            ttk.Label(frame, text=f"Error al cargar datos del backup:\n{e}").pack(anchor="w", padx=10, pady=5)
            return

    try:
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if log_type == "backup":
            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

        if not resultados:
            ttk.Label(frame, text="No se encontraron datos para generar el script.").pack(anchor="w", padx=10, pady=5)
            return

        redo_script = "BEGIN TRANSACTION;\n"
        for row in resultados:
            field, operation, old_value_hex, new_value_hex, table, schema = row

            # Intentar decodificar los valores
            old_value = decode_rowlog(conexion, schema, table, old_value_hex) if old_value_hex else None
            new_value = decode_rowlog(conexion, schema, table, new_value_hex) if new_value_hex else None

            if operation == "LOP_MODIFY_ROW":
                if old_value and new_value:
                    for col_name in new_value.keys():
                        redo_script += f"UPDATE [{schema}].[{table}] SET [{col_name}] = '{new_value[col_name]}' WHERE [{col_name}] = '{old_value.get(col_name, '')}';\n"
                elif old_value or new_value:
                    redo_script += f"-- Operación MODIFICAR con valores parciales en la tabla [{schema}].[{table}]\n"

            elif operation == "LOP_INSERT_ROWS":
                if new_value:
                    columns = ', '.join([f"[{col}]" for col in new_value.keys()])
                    values = ', '.join([f"'{val}'" for val in new_value.values()])
                    redo_script += f"INSERT INTO [{schema}].[{table}] ({columns}) VALUES ({values});\n"
                elif old_value:
                    columns = ', '.join([f"[{col}]" for col in old_value.keys()])
                    values = ', '.join([f"'{val}'" for val in old_value.values()])
                    redo_script += f"INSERT INTO [{schema}].[{table}] ({columns}) VALUES ({values});\n"

            elif operation == "LOP_DELETE_ROWS":
                    if old_value:
                        conditions = ' AND '.join([f"[{col}] = '{val}'" for col, val in old_value.items()])
                        redo_script += f"DELETE FROM [{schema}].[{table}] WHERE {conditions};\n"
                    elif new_value:
                        conditions = ' AND '.join([f"[{col}] = '{val}'" for col, val in new_value.items()])
                        redo_script += f"-- Operación DELETE con valores parciales en la tabla [{schema}].[{table}]\n"

        redo_script += "COMMIT TRANSACTION;\n"


        text_widget = tk.Text(frame, wrap="none", height=20, width=80, font=("Fira Code", 16), bg="white", fg="black")
        text_widget.insert("1.0", redo_script)
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)

        text_widget.configure(state="disabled")

    except Exception as e:
        ttk.Label(frame, text=f"Error al generar el script:\n{e}").pack(anchor="w", padx=10, pady=5)



def definir_contenido_transaction_info(frame, conexion, transaction_id, operation, log_type, backup_path=None):
    """Define el contenido de la pestaña Transaction Information"""
    for widget in frame.winfo_children():
        widget.destroy()

    if not transaction_id:
        label = ttk.Label(frame, text="No se pudo encontrar el Transaction ID.")
        label.pack()
        return

    consulta = ""
    cursor = conexion.cursor()

    if log_type == "online":
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
    elif log_type == "backup":
        if not backup_path:
            backup_path = "/var/opt/mssql/data/EmpleadosDB_Log.bak"

        try:
            cursor.execute(f"""
                SELECT *
                INTO #TempTransactionLog
                FROM fn_dump_dblog(
                    NULL, NULL, N'DISK', 1, N'{backup_path}', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
                    DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
                );
            """)
            conexion.commit()

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
                COALESCE(
                    (SELECT OBJECT_NAME(p.object_id)
                    FROM sys.allocation_units au
                    JOIN sys.partitions p ON au.container_id = p.hobt_id
                    WHERE au.allocation_unit_id = d.AllocUnitId
                    ), 'Unknown') AS "Object",
                'N/A' AS "Parent Schema",
                'N/A' AS "Parent Object",
                COALESCE(SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85))), SYSTEM_USER, 'Unknown User') AS "User",
                CASE
                    WHEN d.[Transaction SID] IS NOT NULL THEN '0x' + CONVERT(VARCHAR(MAX), d.[Transaction SID], 1)
                    ELSE '0x0x01'
                END AS "User ID"
            FROM
                #TempTransactionLog AS d
            LEFT JOIN
                #TempTransactionLog AS begin_xact
                ON d.[Transaction ID] = begin_xact.[Transaction ID] AND begin_xact.Operation = 'LOP_BEGIN_XACT'
            LEFT JOIN
                #TempTransactionLog AS commit_xact
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
                d.[Transaction ID] = '{transaction_id}'
                AND d.Operation = '{operation}'
                AND s.name != 'sys' -- Excluir objetos del esquema sys
                AND (
                    SELECT OBJECT_NAME(p.object_id)
                    FROM sys.allocation_units au
                    JOIN sys.partitions p ON au.container_id = p.hobt_id
                    WHERE au.allocation_unit_id = d.AllocUnitId
                ) IS NOT NULL -- Excluir Unknown
            ORDER BY
                d.[Current LSN];

            """
        except Exception as e:
            ttk.Label(frame, text=f"Error al cargar datos del backup:\n{e}").pack(anchor="w", padx=10, pady=5)
            return

    try:
        cursor.execute(consulta)
        resultado = cursor.fetchone()

        if log_type == "backup":
            cursor.execute("DROP TABLE #TempTransactionLog;")
            conexion.commit()

        if not resultado:
            ttk.Label(frame, text="No se encontró información para esta operación.").pack(anchor="w", padx=10, pady=5)
            return

        if resultado[4] and "." in resultado[4]:
            resultado = list(resultado)
            resultado[4] = resultado[4].split(".")[1]

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
        ttk.Label(frame, text=f"Error al obtener los detalles: {e}").pack(anchor="w", padx=10, pady=5)


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
