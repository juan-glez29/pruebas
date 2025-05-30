import pandas as pd

df_filtered = pd.read_parquet('Facturacion_mod_unido.parquet')
kpis_filtered = pd.read_parquet('kpis_1er_trim.parquet')

df_filtered = df_filtered[df_filtered['fees'] != 0]
df_filtered['mes'] = df_filtered['data_date_part'].str[5:7]

recuperaciones = df_filtered[~df_filtered.palanca_recuperatoria.isin(["CGE", "ANT"])]
gestion = df_filtered[df_filtered.palanca_recuperatoria == "CGE"]

rec = recuperaciones.groupby(["servicer", "mes", "palanca_recuperatoria"], as_index=False).agg({
    "facturacion": "sum"
})
gestion_sum = gestion.groupby(["servicer", "mes", "palanca_recuperatoria"], as_index=False).agg({
    "facturacion": "sum"
})

df_fact = pd.concat([rec, gestion_sum], ignore_index=True)

df_fact = df_fact.rename(columns={
    "palanca_recuperatoria": "Palanca",
    "servicer": "Servicer",
    "facturacion": "Valor"
})

df_pivot = df_fact.pivot_table(
    index=["Servicer", "Palanca"],
    columns="mes",
    values="Valor",
    aggfunc="sum"
).reset_index()

meses = {
    '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
    '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
    '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
}
df_pivot.columns = [meses.get(col, col) for col in df_pivot.columns]
