import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from ConnectDB import conectar_sqlserver

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

        # Cambiar a la base de datos seleccionada
        cursor = conexion.cursor()
        cursor.execute(f"USE {base_datos_seleccionada}")

        # Pasar a la interfaz de transacciones
        mostrar_interfaz_transacciones(conexion)

    btn_confirmar = ttk.Button(ventana, text="Confirmar", command=confirmar_seleccion)
    btn_confirmar.grid(row=1, column=0, columnspan=2, pady=20)

def mostrar_interfaz_transacciones(conexion):
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

    btn_apply = ttk.Button(filtros_frame, text="Apply", command=lambda: actualizar_transacciones(conexion, var_insert.get(), var_update.get(), var_delete.get(), log_type_var.get(), tree))
    btn_apply.grid(row=0, column=5, padx=5)

    btn_desconectar = ttk.Button(filtros_frame, text="Desconectar", command=mostrar_interfaz_conexion)
    btn_desconectar.grid(row=0, column=6, padx=5)

    columnas = [" ", "Operation", "Schema", "Object", "User", "Begin time", "End time", "Transaction ID", "LSN"]
    global tree
    tree = ttk.Treeview(ventana, columns=columnas, show="headings")
    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, width=140)
    tree.grid(row=1, column=0, padx=2, pady=2)

    # Evento de doble clic para abrir detalles de la transacción
    tree.bind("<Double-1>", lambda event: mostrar_detalle_transaccion(tree.item(tree.selection())["values"]))

def mostrar_detalle_transaccion(detalle_transaccion):
    # Crear una ventana secundaria para mostrar los detalles
    ventana_detalle = tk.Toplevel(ventana)
    ventana_detalle.title("Detalle de la Operación")
    ventana_detalle.geometry("800x600")

    # Crear un notebook para los tabs
    notebook = ttk.Notebook(ventana_detalle)
    notebook.pack(fill="both", expand=True)

    # Pestaña de Detalles de Operación
    frame_detalles = ttk.Frame(notebook)
    notebook.add(frame_detalles, text="Operation details")
    definir_contenido_operation_details(frame_detalles, detalle_transaccion)

    # Pestaña de Historial de Fila
    frame_historial = ttk.Frame(notebook)
    notebook.add(frame_historial, text="Row history")
    definir_contenido_row_history(frame_historial, detalle_transaccion)

    # Pestaña de Script de Deshacer (Undo)
    frame_undo = ttk.Frame(notebook)
    notebook.add(frame_undo, text="Undo script")
    definir_contenido_undo_script(frame_undo, detalle_transaccion)

    # Pestaña de Script de Rehacer (Redo)
    frame_redo = ttk.Frame(notebook)
    notebook.add(frame_redo, text="Redo script")
    definir_contenido_redo_script(frame_redo, detalle_transaccion)

    # Pestaña de Información de la Transacción
    frame_informacion = ttk.Frame(notebook)
    notebook.add(frame_informacion, text="Transaction information")
    definir_contenido_transaction_info(frame_informacion, detalle_transaccion)


def definir_contenido_operation_details(frame, detalle_transaccion):
    """Define el contenido de la pestaña Operation details"""
    label = ttk.Label(frame, text=f"Detalles de la operación: {detalle_transaccion}")
    label.pack()

def definir_contenido_row_history(frame, detalle_transaccion):
    """Define el contenido de la pestaña Row history"""
    label = ttk.Label(frame, text="Historial de Fila")
    label.pack()
    # Aquí puedes agregar más widgets y lógica para mostrar el historial de filas

def definir_contenido_undo_script(frame, detalle_transaccion):
    """Define el contenido de la pestaña Undo script"""
    label = ttk.Label(frame, text="Script de Deshacer")
    label.pack()
    # Aquí puedes agregar más widgets y lógica para mostrar el script de deshacer

def definir_contenido_redo_script(frame, detalle_transaccion):
    """Define el contenido de la pestaña Redo script"""
    label = ttk.Label(frame, text="Script de Rehacer")
    label.pack()
    # Aquí puedes agregar más widgets y lógica para mostrar el script de rehacer

def definir_contenido_transaction_info(frame, detalle_transaccion):
    """Define el contenido de la pestaña Transaction information"""
    label = ttk.Label(frame, text="Información de la Transacción")
    label.pack()
    # Aquí puedes agregar más widgets y lógica para mostrar la información de la transacción


def actualizar_transacciones(conexion, show_insert, show_update, show_delete, log_type, tree):
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

    consulta = f"""SELECT
    d.Operation AS "Operation",
    CASE
        WHEN s.name IS NOT NULL THEN s.name
        ELSE 'dbo'
    END AS "Schema",
    o.name AS "Object",
    CASE
        WHEN d.[Transaction SID] IS NOT NULL
            THEN SUSER_SNAME(TRY_CAST(d.[Transaction SID] AS VARBINARY(85)))
        ELSE 'Unknown User'
    END AS "User" ,
    d.[Transaction Begin] AS "Begin Time",
    d.[End Time] AS "End Time",
    d.[Transaction ID] AS "Transaction ID",
    d.[Current LSN] AS "LSN"
FROM
    fn_dblog(NULL, NULL) d
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
ORDER BY
    d.[Current LSN];
    """

    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        if not resultados:
            messagebox.showinfo("Información", "No se encontraron transacciones con los filtros seleccionados.")
        else:
            for index, row in enumerate(resultados, start=1):
                clean_row = (index,) + tuple(str(item).strip("(),'") for item in row)
                tree.insert("", "end", values=clean_row)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudieron cargar las transacciones:\n{e}")

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
