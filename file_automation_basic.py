import os
import re
import warnings
import chardet
import pandas as pd
import xlrd
import openpyxl
import datetime as dt
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#   CONFIGURACIÓN
# ─────────────────────────────────────────────
CARPETA_RAIZ  = "./archivos"
fecha = dt.datetime.now().strftime("%Y%m%d%H%M%S")
ARCHIVO_SALIDA = f"reporte_inventario_{fecha}.xlsx"

EXTENSIONES_DATOS   = {".xls", ".xlsx", ".csv", ".txt"}
EXTENSIONES_CORREO  = {".msg"}
SEPARADORES_CANDIDATOS = [",", "\t", ";", "|"]

PESOS = {
    "estructura_consistente":   20,
    "sin_formulas":             15,
    "pocos_nulos":              20,
    "tipos_consistentes":       15,
    "nombres_columnas_limpios": 10,
    "una_tabla_por_hoja":       10,
    "sin_hojas_vacias":          5,
    "pocas_hojas":               5,
}

# ─────────────────────────────────────────────
#   UTILIDADES
# ─────────────────────────────────────────────
def descubrir_archivos(carpeta_raiz):
    encontrados = []
    for dirpath, _, filenames in os.walk(carpeta_raiz):
        for filename in filenames:
            if filename.startswith("~$"):
                continue
            ext = os.path.splitext(filename)[1].lower()
            ruta = os.path.join(dirpath, filename)
            if ext in EXTENSIONES_DATOS:
                encontrados.append({"ruta": ruta, "tipo": "datos"})
            elif ext in EXTENSIONES_CORREO:
                encontrados.append({"ruta": ruta, "tipo": "correo"})
    return encontrados

def detectar_encoding(datos_bytes=None, ruta=None):
    try:
        muestra = datos_bytes[:50_000] if datos_bytes else open(ruta, "rb").read(50_000)
        resultado = chardet.detect(muestra)
        enc = resultado.get("encoding") or "utf-8"
        return enc.replace("ISO-8859-1", "latin-1").replace("Windows-1252", "cp1252")
    except Exception:
        return "utf-8"

def detectar_separador(texto, n_lineas=5):
    lineas = [l for l in texto.splitlines()[:n_lineas] if l.strip()]
    if not lineas:
        return ","
    mejor_sep, mejor_cols = ",", 0
    for sep in SEPARADORES_CANDIDATOS:
        conteos = [len(l.split(sep)) for l in lineas]
        if len(set(conteos)) == 1 and conteos[0] > mejor_cols:
            mejor_cols, mejor_sep = conteos[0], sep
    return mejor_sep

def leer_csv_txt(datos_bytes=None, ruta=None):
    encoding = detectar_encoding(datos_bytes=datos_bytes, ruta=ruta)
    texto = datos_bytes.decode(encoding, errors="replace") if datos_bytes else open(ruta, "r", encoding=encoding, errors="replace").read()
    sep = detectar_separador(texto)
    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str, on_bad_lines="skip")
    return df, sep, encoding

def es_nombre_columna_limpio(nombre):
    nombre = str(nombre).strip()
    if nombre.startswith("Unnamed") or nombre == "":
        return False
    return not re.search(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑüÜ _\-./()]", nombre)

def primera_fila_con_datos(df_raw):
    for i, row in df_raw.iterrows():
        if row.notna().any():
            return i
    return 0

def analizar_df(df, nombre_hoja="Hoja1"):
    hoja = {
        "nombre_hoja": nombre_hoja,
        "vacia": df is None or df.empty,
        "num_columnas": len(df.columns),
        "nombres_columnas": [str(c) for c in df.columns],
        "pct_nulos_por_columna": df.isnull().mean().mul(100).round(1).to_dict(),
        "tipos_por_columna": {str(c): str(df[c].dtype) for c in df.columns},
    }
    limpias = [c for c in df.columns if es_nombre_columna_limpio(c)]
    hoja["pct_columnas_limpias"] = round(len(limpias) / max(len(df.columns), 1) * 100, 1)
    return hoja

# ─────────────────────────────────────────────
#   ANÁLISIS DE ARCHIVOS
# ─────────────────────────────────────────────
def analizar_excel(ruta, nombre):
    es_xlsx = nombre.lower().endswith(".xlsx")
    engine  = "openpyxl" if es_xlsx else "xlrd"
    resultado = {"archivo": nombre, "ruta": ruta, "formato": "xlsx" if es_xlsx else "xls", "hojas": []}
    try:
        hojas_raw = pd.read_excel(ruta, sheet_name=None, engine=engine)
        for nombre_hoja, df_raw in hojas_raw.items():
            header_row = primera_fila_con_datos(df_raw)
            df = pd.read_excel(ruta, sheet_name=nombre_hoja, skiprows=header_row, engine=engine)
            info = analizar_df(df, nombre_hoja)
            resultado["hojas"].append(info)
    except Exception as e:
        resultado["error"] = str(e)
    return resultado

def analizar_csv_txt(ruta, nombre):
    resultado = {"archivo": nombre, "ruta": ruta, "formato": "csv/txt", "hojas": []}
    try:
        df, sep, enc = leer_csv_txt(ruta=ruta)
        info = analizar_df(df, "datos")
        resultado["hojas"].append(info)
    except Exception as e:
        resultado["error"] = str(e)
    return resultado

def analizar_archivo(item):
    ruta, tipo = item["ruta"], item["tipo"]
    nombre = os.path.basename(ruta)
    if tipo == "datos":
        ext = os.path.splitext(nombre)[1].lower()
        if ext in (".xls", ".xlsx"):
            return analizar_excel(ruta, nombre)
        elif ext in (".csv", ".txt"):
            return analizar_csv_txt(ruta, nombre)
    return {"archivo": nombre, "error": "Tipo no soportado", "hojas": []}

# ─────────────────────────────────────────────
#   SCORING
# ─────────────────────────────────────────────
def calcular_score(analisis):
    hojas = analisis.get("hojas", [])
    if not hojas:
        return {k: 0 for k in PESOS}, 0, "🔴 Muy baja"
    score = {k: PESOS[k] for k in PESOS}
    total = sum(score.values())
    cat = "🟢 Alta" if total >= 80 else "🟡 Media" if total >= 55 else "🟠 Baja" if total >= 30 else "🔴 Muy baja"
    return score, total, cat

# ─────────────────────────────────────────────
#   EXPORTAR REPORTE (Optimizado con conditional formatting)
# ─────────────────────────────────────────────
def exportar_reporte(resultados, adjuntos_correo, ruta_salida):
    filas_scoring, filas_resumen, filas_nulos, filas_errores = [], [], [], []

    for r in resultados:
        if r.get("error") and not r.get("hojas"):
            filas_errores.append({"archivo": r.get("archivo"), "ruta": r.get("ruta"), "error": r["error"]})
            continue
        score_d, score_t, cat = calcular_score(r)
        filas_scoring.append({"archivo": r["archivo"], "score_total (0-100)": score_t, "categoria": cat})
        for h in r.get("hojas", []):
            filas_resumen.append({"archivo": r["archivo"], "hoja": h["nombre_hoja"], "num_columnas": h.get("num_columnas")})
            for col, pct in h.get("pct_nulos_por_columna", {}).items():
                filas_nulos.append({"archivo": r["archivo"], "hoja": h["nombre_hoja"], "columna": col, "pct_nulos": pct})

    df_scoring  = pd.DataFrame(filas_scoring).sort_values("score_total (0-100)", ascending=False)
    df_resumen  = pd.DataFrame(filas_resumen)
    df_nulos    = pd.DataFrame(filas_nulos)
    df_adjuntos = pd.DataFrame(adjuntos_correo)
    df_errores  = pd.DataFrame(filas_errores)

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        df_scoring.to_excel(writer,  sheet_name="Scoring", index=False)
        df_resumen.to_excel(writer,  sheet_name="Resumen", index=False)
        df_nulos.to_excel(writer,    sheet_name="Detalle_Nulos", index=False)
        df_adjuntos.to_excel(writer, sheet_name="Adjuntos_Correos", index=False)
        df_errores.to_excel(writer,  sheet_name="Errores", index=False)

        wb = writer.book
        ws = wb["Scoring"]

        # ── Conditional Formatting para categorías ──
        from openpyxl.formatting.rule import FormulaRule
        from openpyxl.styles import PatternFill

        reglas = {
            "🟢 Alta":  "C6EFCE",
            "🟡 Media": "FFEB9C",
            "🟠 Baja":  "FFCC99",
            "🔴 Muy baja": "FFC7CE",
        }

        col_categoria = None
        for cell in ws[1]:
            if cell.value == "categoria":
                col_categoria = cell.column_letter
                break

        if col_categoria:
            for cat, color in reglas.items():
                formula = f'${col_categoria}2="{cat}"'
                fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                ws.conditional_formatting.add(f"{col_categoria}2:{col_categoria}{ws.max_row}",
                                              FormulaRule(formula=[formula], fill=fill))

        # ── Header en negrita ──
        from openpyxl.styles import Font, Alignment
        for ws_name in wb.sheetnames:
            for cell in wb[ws_name][1]:
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal="center")

    print(f"\n✅ Reporte guardado: {ruta_salida}")


# ─────────────────────────────────────────────
#   MAIN CON PARALELIZACIÓN
# ─────────────────────────────────────────────
def main():
    if not os.path.isdir(CARPETA_RAIZ):
        print(f"❌ Carpeta no encontrada: {CARPETA_RAIZ}")
        return

    encontrados = descubrir_archivos(CARPETA_RAIZ)
    print(f"🔍 Archivos encontrados: {len(encontrados)}")

    resultados = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(analizar_archivo, item): item for item in encontrados}
        for future in as_completed(futures):
            resultados.append(future.result())

    print(f"📊 Procesados: {len(resultados)} archivos")
    exportar_reporte(resultados, [], ARCHIVO_SALIDA)


if __name__ == "__main__":
    main()
