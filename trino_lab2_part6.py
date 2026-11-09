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


# 1. Vérifier l'état initial
print("\n=== SNAPSHOTS AVANT ACTUALISATION ===")

before = sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")

for row in before:
    print(row)

# Éviter une actualisation répétée accidentelle
if len(before) != 1:
    raise SystemExit(
        "La table possède déjà plusieurs snapshots. "
        "Arrête-toi ici et envoie-moi le résultat."
    )

first_snapshot = before[0][0]

initial = sql("""
    SELECT count(*), sum(units)
    FROM lakehouse.gold.top_products
""")[0]

print("État initial :", initial)

assert tuple(initial) == (6, 8615)

# 2. Vider la table Gold
sql("""
    DELETE FROM lakehouse.gold.top_products
""")

print("\nDELETE : terminé")


# 3. Reconstruire les données
sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT
        product,
        count(*) AS orders,
        sum(quantity) AS units,
        rank() OVER (
            ORDER BY sum(quantity) DESC
        ) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")

print("INSERT : terminé")


# 4. Vérifier les nouveaux snapshots
print("\n=== SNAPSHOTS APRES ACTUALISATION ===")

after = sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")

for row in after:
    print(row)


# 5. Comparer version actuelle et ancienne
current = sql("""
    SELECT count(*), sum(units)
    FROM lakehouse.gold.top_products
""")[0]

historical = sql(f"""
    SELECT count(*), sum(units)
    FROM lakehouse.gold.top_products
    FOR VERSION AS OF {first_snapshot}
""")[0]

print("\n=== COMPARAISON DES VERSIONS ===")
print("Version actuelle :", current)
print("Version initiale :", historical)

assert tuple(current) == (6, 8615)
assert historical == initial

print("\n✅ ACTUALISATION ET TIME TRAVEL REUSSIS")

conn.close()