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


# 1. Quantités
silver_units = sql("""
    SELECT sum(quantity)
    FROM lakehouse.silver.orders
""")

gold_month_units = sql("""
    SELECT sum(units)
    FROM lakehouse.gold.orders_by_state_month
""")

gold_product_units = sql("""
    SELECT sum(units)
    FROM lakehouse.gold.top_products
""")


# 2. Nombre de commandes
silver_orders = sql("""
    SELECT count(*)
    FROM lakehouse.silver.orders
""")

gold_month_orders = sql("""
    SELECT sum(orders)
    FROM lakehouse.gold.orders_by_state_month
""")

gold_user_orders = sql("""
    SELECT sum(orders)
    FROM lakehouse.gold.user_activity
""")


# 3. Nombre d'utilisateurs
silver_users = sql("""
    SELECT count(*)
    FROM lakehouse.silver.users
""")

gold_users = sql("""
    SELECT count(*)
    FROM lakehouse.gold.user_activity
""")


# 4. Afficher les résultats
print("\n=== VALIDATION DES QUANTITES ===")
print("Silver :", silver_units)
print("Gold par mois :", gold_month_units)
print("Gold produits :", gold_product_units)

print("\n=== VALIDATION DES COMMANDES ===")
print("Silver :", silver_orders)
print("Gold par mois :", gold_month_orders)
print("Gold utilisateurs :", gold_user_orders)

print("\n=== VALIDATION DES UTILISATEURS ===")
print("Silver :", silver_users)
print("Gold :", gold_users)


# 5. Vérifier automatiquement
assert silver_units == gold_month_units
assert silver_units == gold_product_units

assert silver_orders == gold_month_orders
assert silver_orders == gold_user_orders

assert silver_users == gold_users

print("\n✅ TOUTES LES VALIDATIONS SONT REUSSIES")

conn.close()