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

def mostrar_transacciones(conexion):
    for widget in ventana.winfo_children():
        widget.destroy()

    ventana.geometry("1140x600")
    ventana.title("Listado de Operaciones del Log")

    filtros_frame = tk.Frame(ventana)
    filtros_frame.grid(row=0, column=0, padx=10, pady=10, sticky="w")

    var_insert = tk.BooleanVar(value=True)
    var_update = tk.BooleanVar(value=True)
    var_delete = tk.BooleanVar(value=True)

    tk.Checkbutton(filtros_frame, text="Insert", variable=var_insert).grid(row=0, column=0, padx=5)
    tk.Checkbutton(filtros_frame, text="Update", variable=var_update).grid(row=0, column=1, padx=5)
    tk.Checkbutton(filtros_frame, text="Delete", variable=var_delete).grid(row=0, column=2, padx=5)

    btn_apply = tk.Button(filtros_frame, text="Apply", command=lambda: actualizar_transacciones(conexion, var_insert.get(), var_update.get(), var_delete.get()))
    btn_apply.grid(row=0, column=3, padx=2)

    columnas = ["Operation", "Schema", "Object", "User", "Begin time", "End time", "Transaction ID", "LSN"]
    tree = ttk.Treeview(ventana, columns=columnas, show="headings")
    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, width=140)
    tree.grid(row=1, column=0, padx=2, pady=2)

    actualizar_transacciones(conexion, True, True, True, tree)

def actualizar_transacciones(conexion, show_insert, show_update, show_delete, tree):
    for item in tree.get_children():
        tree.delete(item)

    consulta = """
    SELECT
        operation, schema_name, object_name, user_name, begin_time, end_time, transaction_id, lsn
    FROM
        fn_dblog(NULL, NULL)  -- Debes modificar esta consulta según tu fuente de log de transacciones
    WHERE
        (operation = 'INSERT' AND ? = 1) OR
        (operation = 'UPDATE' AND ? = 1) OR
        (operation = 'DELETE' AND ? = 1)
    """
    cursor = conexion.cursor()
    cursor.execute(consulta, show_insert, show_update, show_delete)

    for row in cursor.fetchall():
        tree.insert("", "end", values=row)

def conectar():
    servidor = entry_server.get()
    puerto = entry_port.get()
    usuario = entry_user.get()
    contrasena = entry_password.get()
    conexion = conectar_sqlserver(servidor, puerto, usuario, contrasena, "master")

    if conexion:
        print("Conexión exitosa")
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sys.databases;")
        bases_datos = [row[0] for row in cursor.fetchall()]
        abrir_ventana_popup(bases_datos, conexion)
    else:
        messagebox.showerror("Error", "No se pudo establecer la conexión")

def abrir_ventana_popup(bases_datos, conexion):
    popup = tk.Toplevel()
    popup.title("Seleccionar base de datos y opciones")
    popup.geometry("400x300")

    tk.Label(popup, text="Database:").grid(row=0, column=0, sticky="w", padx=10, pady=5)
    combo_database = ttk.Combobox(popup, values=bases_datos, width=28)
    combo_database.grid(row=0, column=1, padx=10, pady=5)
    combo_database.current(0)

    var_log_type = tk.StringVar()
    tk.Radiobutton(popup, text="Online transaction log", variable=var_log_type, value="online").grid(row=1, column=0, columnspan=2, sticky="w", padx=10)
    tk.Radiobutton(popup, text="Backup file", variable=var_log_type, value="backup").grid(row=2, column=0, columnspan=2, sticky="w", padx=10)

    tk.Label(popup, text="From:").grid(row=3, column=0, sticky="w", padx=10, pady=5)
    entry_from = tk.Entry(popup, width=12)
    entry_from.grid(row=3, column=1, sticky="w", padx=10, pady=5)

    tk.Label(popup, text="To:").grid(row=4, column=0, sticky="w", padx=10, pady=5)
    entry_to = tk.Entry(popup, width=12)
    entry_to.grid(row=4, column=1, sticky="w", padx=10, pady=5)

    btn_confirmar = tk.Button(popup, text="Confirmar", command=lambda: [popup.destroy(), mostrar_transacciones(conexion)])
    btn_confirmar.grid(row=5, column=0, columnspan=2, pady=20)

def iniciar_interfaz():
    global ventana, entry_server, entry_port, entry_user, entry_password
    ventana = tk.Tk()
    ventana.title("Conexión a SQL Server")
    ventana.geometry("550x400")

    tk.Label(ventana, text="Server:").grid(row=0, column=0, sticky="w", padx=10, pady=5)
    entry_server = tk.Entry(ventana, width=30)
    entry_server.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(ventana, text="Port:").grid(row=1, column=0, sticky="w", padx=10, pady=5)
    entry_port = tk.Entry(ventana, width=30)
    entry_port.insert(0, "1433")
    entry_port.grid(row=1, column=1, padx=10, pady=5)

    tk.Label(ventana, text="User:").grid(row=2, column=0, sticky="w", padx=10, pady=5)
    entry_user = tk.Entry(ventana, width=30)
    entry_user.grid(row=2, column=1, padx=10, pady=5)

    tk.Label(ventana, text="Password:").grid(row=3, column=0, sticky="w", padx=10, pady=5)
    entry_password = tk.Entry(ventana, show="*", width=30)
    entry_password.grid(row=3, column=1, padx=10, pady=5)

    btn_conectar = tk.Button(ventana, text="Conectarse", command=conectar)
    btn_conectar.grid(row=4, column=0, columnspan=2, pady=20)

    ventana.mainloop()

def start_watchdog():
    observer = Observer()
    event_handler = ReloadOnChangeHandler()
    observer.schedule(event_handler, path=".", recursive=True)
    observer.start()

    try:
        iniciar_interfaz()
    except KeyboardInterrupt:
        print("Interrumpido por el usuario")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watchdog()
