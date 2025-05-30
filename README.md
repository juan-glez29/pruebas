tengo este codigo, quiero que hagas la misma tabla, pero que en la Columna Concepto, pongas los valores de columna palanca_recuperatoria, que esta presente tanto en df_filtered como en kpis_filtered, en lugar de "Total Facturación", "Base facturación", "Fact/Base", "Fact/Rec", "KPI's Recuperac", "KPI's ASR", "Fact. Palanca", "Base Fact Palanca", "Fact. Com Gest", "Base Fact Com Gest

import pandas as pd

df_filtered = pd.read_parquet('Facturacion_mod_unido.parquet')
kpis_filtered = pd.read_parquet('kpis_1er_trim.parquet')

# Eliminamos palancas con fees = 0
df_filtered = df_filtered[df_filtered['fees']!=0]

# Extraer el mes de 'data_date_part'
df_filtered['mes'] = df_filtered['data_date_part'].str[5:7]

# Separar Recuperaciones vs Gestión
recuperaciones = df_filtered[
    ~df_filtered.palanca_recuperatoria.isin(["CGE", "ANT"])
]

gestion = df_filtered[df_filtered.palanca_recuperatoria == "CGE"]

summary = {}
columnas = ["importe_palanca", "facturacion"]

for var in columnas:
    summary[var] = (var, "sum")

# Agrupar por servicer, mes y sumar
rec = recuperaciones.groupby(["servicer", "mes"], as_index=False).agg(**summary)
importe_gestionado = gestion.groupby(["servicer", "mes"], as_index=False).agg(**summary)

# Renombrar columnas
rec.columns = [
    "servicer",
    "mes",
    "Base Fact Palanca",
    "Fact. Palanca",
]

importe_gestionado.columns = [
    "servicer",
    "mes",
    "Base Fact Com Gest",
    "Fact. Com Gest",
]

# Merge y cálculo de total facturación
df_resumen_mes = rec.merge(importe_gestionado, on=["servicer", "mes"], how="left", suffixes=('_rec', '_gest'))
df_resumen_mes = df_resumen_mes.fillna(0)

# Calcular total facturación y base facturación
df_resumen_mes["Total Facturación"] = df_resumen_mes["Fact. Palanca"] + df_resumen_mes["Fact. Com Gest"]
df_resumen_mes["Base facturación"] = df_resumen_mes["Base Fact Palanca"] + df_resumen_mes["Base Fact Com Gest"]
df_resumen_mes["Fact/Base"] = df_resumen_mes["Total Facturación"] / df_resumen_mes["Base facturación"]


salidas = kpis_filtered.groupby("servicer", as_index=False)[["total_salidas","salida_fall"]].sum()
# salidas["total_salidas"] = salidas["total_salidas"]
salidas.loc[(salidas["servicer"] == "AAM"), "servicer"] = "DOVALUE"
salidas.loc[(salidas["servicer"] == "AKT"), "servicer"] = "INTRUM"
salidas.loc[(salidas["servicer"] == "ALI"), "servicer"] = "DIGLO"
salidas.loc[(salidas["servicer"] == "AXA"), "servicer"] = "AXACTOR"
salidas.loc[(salidas["servicer"] == "GES"), "servicer"] = "GESCOBRO"
salidas = salidas[
    salidas["servicer"].isin(["DOVALUE", "DIGLO", "INTRUM", "AXACTOR", "GESCOBRO"])
]

df_resumen_mes = df_resumen_mes.merge(
    salidas[["servicer", "total_salidas","salida_fall"]], on="servicer", how="left"
)

df_resumen_mes.rename(columns={"total_salidas": "KPI's Recuperac", "salida_fall": "KPI's ASR"}, inplace=True)

df_resumen_mes["Fact/Rec"] = (
    df_resumen_mes["Total Facturación"]/df_resumen_mes["KPI's Recuperac"]
)

# Melt para transformar de ancho a largo
df_resumen_mes = df_resumen_mes.melt(id_vars=["servicer", "mes"], value_vars=["Total Facturación", "Base facturación", "Fact/Base", "Fact/Rec", "KPI's Recuperac", "KPI's ASR", "Fact. Palanca", "Base Fact Palanca", "Fact. Com Gest", "Base Fact Com Gest"])

# Renombrar columnas
df_resumen_mes = df_resumen_mes.rename(columns={"variable": "Concepto", "value": "Valor"})

# Pivot para transformar de largo a ancho con meses como columnas
df_resumen_pivot = df_resumen_mes.pivot(index=["servicer", "Concepto"], columns="mes", values="Valor").reset_index()

# Mapear meses a nombres de meses
meses = {
    '01': 'Enero',
    '02': 'Febrero',
    '03': 'Marzo',
    '04': 'Abril',
    '05': 'Mayo',
    '06': 'Junio',
    '07': 'Julio',
    '08': 'Agosto',
    '09': 'Septiembre',
    '10': 'Octubre',
    '11': 'Noviembre',
    '12': 'Diciembre'
}

df_resumen_pivot.columns = [col if col not in meses else meses[col] for col in df_resumen_pivot.columns]

# df_resumen_pivot['Total']= df_resumen_pivot['']
