import os
import trino

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user=os.environ["KUBERNETES_NAMESPACE"],
    catalog="lakehouse",
)


def sql(query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


# 1. Créer la table partitionnée
sql("""
CREATE TABLE IF NOT EXISTS
    lakehouse.gold.orders_by_state_month_part
WITH (
    partitioning = ARRAY['month(month)']
)
AS
SELECT *
FROM lakehouse.gold.orders_by_state_month
""")

print("\n=== TABLE PARTITIONNEE CREEE ===")


# 2. Comparer les nombres de lignes
for table in [
    "orders_by_state_month",
    "orders_by_state_month_part"
]:
    result = sql(f"""
        SELECT count(*)
        FROM lakehouse.gold.{table}
    """)

    print(table, ":", result[0][0])


# 3. Comparer les fichiers Iceberg
for table in [
    "orders_by_state_month",
    "orders_by_state_month_part"
]:
    print(f"\n=== FICHIERS : {table} ===")

    files = sql(f"""
        SELECT file_path, record_count, file_size_in_bytes
        FROM lakehouse.gold."{table}$files"
    """)

    print("Nombre de fichiers :", len(files))

    for row in files:
        print(row)


# 4. Comparer EXPLAIN ANALYZE
for table in [
    "orders_by_state_month",
    "orders_by_state_month_part"
]:
    print(f"\n=== EXPLAIN ANALYZE : {table} ===")

    plan = sql(f"""
        EXPLAIN ANALYZE
        SELECT sum(units)
        FROM lakehouse.gold.{table}
        WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'
    """)[0][0]

    # Afficher uniquement les statistiques utiles
    for line in plan.splitlines():
        if any(term in line for term in [
            "Execution:",
            "TableScan",
            "ScanFilter",
            "Input:",
            "Physical input:",
            "Splits:"
        ]):
            print(line.strip())

conn.close()