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
    return cursor.fetchone()[0]


# 1. Comparer le nombre de commandes
silver_orders = sql("""
SELECT count(*)
FROM lakehouse.silver.orders
""")

fact_orders = sql("""
SELECT count(*)
FROM lakehouse.gold.fact_orders
""")

print("\n=== VALIDATION DES COMMANDES ===")
print("Silver :", silver_orders)
print("Fact :", fact_orders)


# 2. Comparer les quantités
silver_units = sql("""
SELECT sum(quantity)
FROM lakehouse.silver.orders
""")

fact_units = sql("""
SELECT sum(quantity)
FROM lakehouse.gold.fact_orders
""")

print("\n=== VALIDATION DES QUANTITES ===")
print("Silver :", silver_units)
print("Fact :", fact_units)


# 3. Vérifier les références aux dimensions
unmatched = sql("""
SELECT count(*)
FROM lakehouse.gold.fact_orders f

LEFT JOIN lakehouse.gold.dim_user u
    ON f.user_key = u.user_key

LEFT JOIN lakehouse.gold.dim_product p
    ON f.product_key = p.product_key

LEFT JOIN lakehouse.gold.dim_date d
    ON f.date_key = d.date_key

WHERE u.user_key IS NULL
   OR p.product_key IS NULL
   OR d.date_key IS NULL
""")

print("\n=== INTEGRITE REFERENTIELLE ===")
print("Commandes sans dimension correspondante :", unmatched)


# 4. Assertions
assert silver_orders == fact_orders
assert silver_units == fact_units
assert unmatched == 0

print("\n✅ TOUTES LES VALIDATIONS DU STAR SCHEMA SONT REUSSIES")

conn.close()