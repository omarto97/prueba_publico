import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import pandas as pd

# ==========================================
# DEFINICIÓN DE LA ESTRUCTURA (Matriz de Reglas)
# ==========================================
# Cada campo tiene: (Nombre, Tipo, Obligatoriedad, Tamaño)
REGLAS = [
    ('F_NUMERO_REG', 'Numérico', 'Si', 7),
    ('F_TIPO_REG', 'Numérico', 'Si', 4),
    ('F_SUBTIPO_REG', 'Numérico', 'Si', 2),
    ('F_VERSION_REG', 'Numérico', 'Si', 2),
    ('F_CIA', 'Numérico', 'Si', 3),
    ('F_ACTUALIZA_REG', 'Numérico', 'Si', 1),
    ('F201_ID_TERCERO', 'Alfanumérico', 'Si', 15),
    ('F201_ID_SUCURSAL', 'Alfanumérico', 'Si', 3),
    ('F201_IND_ESTADO_ACTIVO', 'Numérico', 'Si', 1),
    ('F201_DESCRIPCION_SUCURSAL', 'Alfanumérico', 'Si', 40),
    ('F201_ID_MONEDA', 'Alfanumérico', 'Si', 3),
    ('F201_ID_VENDEDOR', 'Alfanumérico', 'No', 4),
    ('F201_IND_CALIFICACION', 'Alfanumérico', 'Si', 1),
    ('F201_ID_COND_PAGO', 'Alfanumérico', 'No', 3),
    ('F201_DIAS_GRACIA', 'Numérico', 'No', 3),
    ('F201_CUPO_CREDITO', 'Cupo', 'No', 21), # Caso especial formato
    ('F201_ID_CLIENTE_CORP', 'Alfanumérico', 'Dep', 15),
    ('F201_ID_SUCURSAL_CORP', 'Alfanumérico', 'Dep', 3),
    ('F201_ID_TIPO_CLI', 'Alfanumérico', 'Si', 4),
    ('F201_ID_GRUPO_DSCTO', 'Alfanumérico', 'No', 4),
    ('F201_ID_LISTA_PRECIO', 'Alfanumérico', 'Dep', 3),
    ('F201_IND_PEDIDO_BACKORDER', 'Numérico', 'Dep', 1),
    ('F201_PORC_EXCESO_VENTA', 'Decimal_4_2', 'Dep', 7),
    ('F201_PORC_MIN_MARGEN', 'Decimal_4_2', 'Dep', 7),
    ('F201_PORC_MAX_MARGEN', 'Decimal_4_2', 'Dep', 7),
    ('F201_IND_BLOQUEADO', 'Numérico', 'Dep', 1),
    ('F201_IND_BLOQUEO_CUPO', 'Numérico', 'Dep', 1),
    ('F201_IND_BLOQUEO_MORA', 'Numérico', 'Dep', 1),
    ('F201_IND_FACTURA_UNIFICADA', 'Numérico', 'Dep', 1),
    ('F201_ID_CO_FACTURA', 'Alfanumérico', 'No', 3),
    ('F201_NOTAS', 'Alfanumérico', 'No', 255),
    ('F015_CONTACTO', 'Alfanumérico', 'No', 50),
    ('F015_DIRECCION1', 'Alfanumérico', 'No', 40),
    ('F015_DIRECCION2', 'Alfanumérico', 'No', 40),
    ('F015_DIRECCION3', 'Alfanumérico', 'No', 40),
    ('F015_ID_PAIS', 'Alfanumérico', 'No', 3),
    ('F015_ID_DEPTO', 'Alfanumérico', 'Dep', 2),
    ('F015_ID_CIUDAD', 'Alfanumérico', 'Dep', 3),
    ('F015_ID_BARRIO', 'Alfanumérico', 'No', 40),
    ('F015_TELEFONO', 'Alfanumérico', 'No', 20),
    ('F015_FAX', 'Alfanumérico', 'No', 20),
    ('F015_COD_POSTAL', 'Alfanumérico', 'No', 10),
    ('F015_EMAIL', 'Alfanumérico', 'No', 255),
    ('F201_FECHA_INGRESO', 'Fecha', 'Si', 8),
    ('F201_ID_CO_MOVTO_FACTURA', 'Alfanumérico', 'No', 3),
    ('F201_ID_UN_MOVTO_FACTURA', 'Alfanumérico', 'No', 20),
    ('F201_ID_PARAMETRO_EDI', 'Alfanumérico', 'No', 4),
    ('F201_CODIGO_EAN', 'Alfanumérico', 'No', 35),
    ('f201_fecha_cupo', 'Fecha', 'No', 8),
    ('f201_porc_tolerancia', 'Decimal_4_2', 'No', 7),
    ('f201_dia_maximo_factura', 'Numérico', 'No', 2),
    ('f201_id_motivo_bloqueo', 'Alfanumérico', 'No', 3),
    ('f201_id_cobrador', 'Alfanumérico', 'No', 4),
    ('f201_ind_compromiso_um_emp', 'Numérico', 'No', 1),
    ('f201_ind_anticipo_terc_corp', 'Numérico', 'No', 1),
    ('f015_celular', 'Alfanumérico', 'No', 50),
    ('f201_valida_cupo_despacho', 'Numérico', 'Si', 1),
    ('f201_id_portafolio_edi', 'Alfanumérico', 'No', 10),
    ('f201_frecuencia_entrega', 'Frecuencia', 'Si', 7),
    ('f201_id_cia_cliente_corp', 'Numérico', 'Dep', 3),
    ('f201_ind_valida_cartera_des', 'Numérico', 'Si', 1),
    ('f201_ind_exceso_venta_adic', 'Numérico', 'No', 1)
]

def limpiar_dato(val):
    if pd.isna(val) or str(val).strip().upper() in ['NAN', 'NAT', '']:
        return ""
    return str(val).strip()

# ==========================================
# VALIDADOR Y FORMATEADOR POR FILA
# ==========================================
def procesar_fila(fila, num_fila):
    errores = []
    linea_chunks = []
    
    for nombre, tipo, obl, tamano in REGLAS:
        # Intentar obtener el dato sin importar mayúsculas/minúsculas en el encabezado de Excel
        dict_lower = {k.lower(): v for k, v in fila.items()}
        val = limpiar_dato(dict_lower.get(nombre.lower(), ""))
        
        # --- 1. VALIDACIONES DE OBLIGATORIEDAD Y DEPENDENCIAS ---
        es_vacio = (val == "")
        
        if obl == 'Si' and es_vacio:
            errores.append([num_fila, nombre, "El campo es obligatorio."])
            continue
            
        # Dependencias específicas
        if nombre in ['F201_ID_CLIENTE_CORP', 'F201_ID_SUCURSAL_CORP']:
            id_cia_corp = limpiar_dato(dict_lower.get('f201_id_cia_cliente_corp', ""))
            if id_cia_corp != "" and es_vacio:
                errores.append([num_fila, nombre, "Obligatorio porque 'f201_id_cia_cliente_corp' tiene datos."])
                
        if nombre == 'f201_id_cia_cliente_corp':
            corp = limpiar_dato(dict_lower.get('f201_id_cliente_corp', ""))
            suc_corp = limpiar_dato(dict_lower.get('f201_id_sucursal_corp', ""))
            if (corp != "" or suc_corp != "") and es_vacio:
                errores.append([num_fila, nombre, "Obligatorio si se envía cliente o sucursal corporativa."])

        if nombre == 'F015_ID_DEPTO' and limpiar_dato(dict_lower.get('f015_id_pais', "")) != "" and es_vacio:
            errores.append([num_fila, nombre, "Obligatorio porque existe el campo País."])
            
        if nombre == 'F015_ID_CIUDAD' and limpiar_dato(dict_lower.get('f015_id_depto', "")) != "" and es_vacio:
            errores.append([num_fila, nombre, "Obligatorio porque existe el campo Departamento."])

        # --- 2. VALIDACIONES DE TIPO Y FORMATO + RELLENO (PADDING) ---
        chunk_formateado = ""
        
        if es_vacio:
            # Rellenar según la nota general si está vacío
            if tipo in ['Numérico', 'Decimal_4_2', 'Cupo']:
                chunk_formateado = "0" * tamano
            else:
                chunk_formateado = " " * tamano
        else:
            # Validar longitud máxima general
            if tipo != 'Cupo' and len(val) > tamano:
                errores.append([num_fila, nombre, f"El tamaño supera el máximo permitido ({tamano})."])
            
            # Validaciones específicas por tipo
            if tipo == 'Numérico':
                if not val.isdigit():
                    errores.append([num_fila, nombre, "Debe ser un valor estrictamente numérico entero."])
                chunk_formateado = val.zfill(tamano)
                
            elif tipo == 'Alfanumérico':
                # Valores fijos específicos
                if nombre == 'F201_IND_CALIFICACION' and val not in ['A', 'B', 'C']:
                    errores.append([num_fila, nombre, "Debe ser exactamente 'A', 'B' o 'C'."])
                chunk_formateado = val.ljust(tamano)[:tamano]
                
            elif tipo == 'Fecha':
                if not re.match(r"^\d{8}$", val):
                    errores.append([num_fila, nombre, "Formato de fecha inválido. Debe ser AAAAMMDD."])
                chunk_formateado = val.zfill(tamano)
                
            elif tipo == 'Frecuencia':
                if not re.match(r"^[01]{7}$", val):
                    errores.append([num_fila, nombre, "Formato inválido. Deben ser 7 dígitos de unos y ceros (ej. 1000001)."])
                chunk_formateado = val
                
            elif tipo == 'Decimal_4_2':
                # Formato esperado: 0000.00 -> total 7 caracteres incluyendo el punto
                if not re.match(r"^\d{1,4}\.\d{2}$", val):
                    try:
                        val_float = float(val)
                        val = f"{val_float:07.2f}"
                    except ValueError:
                        errores.append([num_fila, nombre, "Formato decimal inválido. Debe ser (4 enteros + punto + 2 decimales: 0000.00)."])
                chunk_formateado = val.zfill(tamano)
                
            elif tipo == 'Cupo':
                # Formato: signo (+/-) + 15 enteros + punto + 4 decimales = 21 caracteres
                # Ejemplo: +000000000000000.0000
                if not re.match(r"^[+-]\d{15}\.\d{4}$", val):
                    try:
                        # Si meten un número normal en excel, intentar formatearlo automáticamente
                        signo = "+" if float(val) >= 0 else "-"
                        absoluto = abs(float(val))
                        val = f"{signo}{absoluto:020.4f}" # 15 enteros + 1 punto + 4 dec = 20 de ancho para el float
                    except ValueError:
                        errores.append([num_fila, nombre, "Formato inválido. Debe ser signo + 15 enteros + punto + 4 decimales."])
                if len(val) != 21:
                     errores.append([num_fila, nombre, "El formato de Cupo de crédito procesado no cumple los 21 caracteres requeridos."])
                chunk_formateado = val

        linea_chunks.append(chunk_formateado)
        
    return errores, "".join(linea_chunks)

# ==========================================
# LOGICA DE CONTROL DE LA APLICACIÓN
# ==========================================
def procesar_archivo():
    ruta_excel = filedialog.askopenfilename(
        title="Selecciona el archivo Excel",
        filetypes=[("Archivos de Excel", "*.xlsx *.xls")]
    )
    if not ruta_excel:
        return

    try:
        df = pd.read_excel(ruta_excel, dtype=str)
        
        # Validar que al menos existan columnas requeridas básicas
        columnas_excel = [str(c).strip().lower() for c in df.columns]
        columnas_requeridas = [r[0].lower() for r in REGLAS if r[2] == 'Si']
        
        faltantes = [orig for orig, low in zip([r[0] for r in REGLAS if r[2] == 'Si'], columnas_requeridas) if low not in columnas_excel]
        if faltantes:
            messagebox.showerror("Estructura Inválida", f"Al Excel le faltan las siguientes columnas obligatorias:\n{', '.join(faltantes)}")
            return

        todos_los_errores = []
        todas_las_lineas = []
        
        for index, fila in df.iterrows():
            num_fila = index + 2  # Fila del excel real (Excel empieza en 1, fila 1 es cabecera)
            errores_fila, linea_txt = procesar_fila(fila.to_dict(), num_fila)
            
            if errores_fila:
                todos_los_errores.extend(errores_fila)
            else:
                todas_las_lineas.append(linea_txt)
                
        if todos_los_errores:
            df_errores = pd.DataFrame(todos_los_errores, columns=['Fila', 'Columna', 'Descripción del Error'])
            ruta_guardar_errores = filedialog.asksaveasfilename(
                title="Guardar Reporte de Errores",
                defaultextension=".xlsx",
                filetypes=[("Excel", "*.xlsx")],
                initialfile="Reporte_Errores.xlsx"
            )
            if ruta_guardar_errores:
                df_errores.to_excel(ruta_guardar_errores, index=False)
                messagebox.showerror("Errores Encontrados", f"Se detectaron inconsistencias en las reglas.\nReporte exportado en:\n{ruta_guardar_errores}")
        else:
            fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_defecto = f"Resultado_{fecha_hora}.txt"
            
            ruta_guardar_txt = filedialog.asksaveasfilename(
                title="Guardar Archivo Plano Posicional",
                defaultextension=".txt",
                filetypes=[("Archivo de Texto", "*.txt")],
                initialfile=nombre_defecto
            )
            
            if ruta_guardar_txt:
                with open(ruta_guardar_txt, "w", encoding="utf-8", newline="\r\n") as f:
                    f.write("\n".join(todas_las_lineas))
                messagebox.showinfo("Éxito", f"¡Archivo plano generado con éxito!\nRegistros procesados: {len(todas_las_lineas)}")

    except Exception as e:
        messagebox.showerror("Error Crítico", f"No se pudo procesar el documento:\n{str(e)}")

# ==========================================
# INTERFAZ GRÁFICA
# ==========================================
root = tk.Tk()
root.title("Convertidor de Estructura Fija 201")
root.geometry("400x160")
root.resizable(False, False)

frame = tk.Frame(root, pady=20)
frame.pack()

label = tk.Label(frame, text="Validador de Clientes & Sucursales (Estructura Fija)", font=("Arial", 10, "bold"))
label.pack(pady=5)

label_sub = tk.Label(frame, text="Carga un Excel para transformarlo en archivo plano .txt", fg="gray")
label_sub.pack(pady=5)

btn_cargar = tk.Button(
    frame, 
    text="📂 Cargar y Validar Archivo", 
    command=procesar_archivo, 
    bg="#1E88E5", 
    fg="white", 
    font=("Arial", 11, "bold"), 
    padx=15, 
    pady=8,
    cursor="hand2"
)
btn_cargar.pack(pady=10)

root.mainloop()