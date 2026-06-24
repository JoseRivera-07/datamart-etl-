"""
Exploración de datos (EDA) - DataMart S.A.S.
Ejecutar antes de construir el pipeline para entender las fuentes.
"""
import pandas as pd

# ── CSV 1: data.csv ───────────────────────────────────────────────────────────
print("=" * 60)
print("CSV 1: data.csv")
print("=" * 60)

df1 = pd.read_csv('data/data.csv', encoding='latin1')

print(f"\nDimensiones: {df1.shape}")
print(f"\nColumnas: {df1.columns.tolist()}")
print(f"\nTipos de dato:\n{df1.dtypes}")
print(f"\nNulos por columna:\n{df1.isnull().sum()}")
print(f"\nDuplicados: {df1.duplicated().sum()}")
print(f"\nCantidades negativas: {(df1['Quantity'] < 0).sum()}")
print(f"\nUnitPrice <= 0: {(df1['UnitPrice'] <= 0).sum()}")
print(f"\nPaíses únicos: {df1['Country'].nunique()}")
print(f"\nMuestra:\n{df1.head(3)}")

# ── CSV 2: online_retail.csv ──────────────────────────────────────────────────
print("\n" + "=" * 60)
print("CSV 2: online_retail.csv")
print("=" * 60)

df2 = pd.read_csv('data/online_retail.csv', encoding='latin1')

print(f"\nDimensiones: {df2.shape}")
print(f"\nColumnas: {df2.columns.tolist()}")
print(f"\nTipos de dato:\n{df2.dtypes}")
print(f"\nNulos por columna:\n{df2.isnull().sum()}")
print(f"\nDuplicados: {df2.duplicated().sum()}")
print(f"\nCantidades negativas: {(df2['Quantity'] < 0).sum()}")
print(f"\nPrice <= 0: {(df2['Price'] <= 0).sum()}")
print(f"\nPaíses únicos: {df2['Country'].nunique()}")
print(f"\nMuestra:\n{df2.head(3)}")

# ── Solapamiento de fechas ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("Solapamiento de fechas entre fuentes")
print("=" * 60)

df1['InvoiceDate'] = pd.to_datetime(df1['InvoiceDate'])
df2['InvoiceDate'] = pd.to_datetime(df2['InvoiceDate'])

print(f"\nCSV 1 - Rango: {df1['InvoiceDate'].min()} → {df1['InvoiceDate'].max()}")
print(f"CSV 2 - Rango: {df2['InvoiceDate'].min()} → {df2['InvoiceDate'].max()}")