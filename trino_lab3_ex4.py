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


def show(title, query):
    print(f"\n=== {title} ===")

    rows = sql(query)

    for row in rows:
        print(row)

    return rows


# 1. Snapshots avant optimisation
before_snapshots = show(
    "SNAPSHOTS AVANT OPTIMIZE",
    """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
    """
)


# 2. Fichiers avant optimisation
before_files = show(
    "FICHIERS AVANT OPTIMIZE",
    """
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
    """
)

print("\nNombre de fichiers avant :", len(before_files))


# 3. Exécuter OPTIMIZE
print("\n=== EXECUTION OPTIMIZE ===")

cursor = conn.cursor()

cursor.execute("""
ALTER TABLE lakehouse.gold.top_products
EXECUTE optimize
""")

print("OPTIMIZE : terminé")


# 4. Snapshots après optimisation
after_snapshots = show(
    "SNAPSHOTS APRES OPTIMIZE",
    """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
    """
)


# 5. Fichiers après optimisation
after_files = show(
    "FICHIERS APRES OPTIMIZE",
    """
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
    """
)

print("\nNombre de fichiers après :", len(after_files))


# 6. Vérifier les données
result = sql("""
SELECT count(*), sum(units)
FROM lakehouse.gold.top_products
""")[0]

print("\n=== VALIDATION ===")
print("Produits :", result[0])
print("Unités :", result[1])

assert tuple(result) == (6, 8615)

print("\n✅ OPTIMIZE EXECUTE ET DONNEES CONSERVEES")

conn.close()