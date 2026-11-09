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


# 1. Créer la table de faits
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders AS
SELECT
    o.order_id,
    u.user_key,
    p.product_key,
    CAST(
        date_format(o.ordered_at, '%Y%m%d')
        AS integer
    ) AS date_key,
    o.ordered_at,
    o.quantity
FROM lakehouse.silver.orders o
LEFT JOIN lakehouse.gold.dim_user u
    ON u.user_id = o.user_id
LEFT JOIN lakehouse.gold.dim_product p
    ON p.product = o.product
""")

print("fact_orders : créée")


# 2. Vérifier les quatre tables
print("\n=== TABLES DU SCHEMA EN ETOILE ===")

results = sql("""
SELECT 'dim_user' AS table_name, count(*) AS rows
FROM lakehouse.gold.dim_user

UNION ALL

SELECT 'dim_product', count(*)
FROM lakehouse.gold.dim_product

UNION ALL

SELECT 'dim_date', count(*)
FROM lakehouse.gold.dim_date

UNION ALL

SELECT 'fact_orders', count(*)
FROM lakehouse.gold.fact_orders
""")

for row in results:
    print(row)


# 3. Afficher quelques commandes
print("\n=== EXEMPLE FACT ORDERS ===")

results = sql("""
SELECT
    order_id,
    user_key,
    product_key,
    date_key,
    quantity
FROM lakehouse.gold.fact_orders
LIMIT 10
""")

for row in results:
    print(row)


# 4. Vérifier les clés manquantes
print("\n=== CLES MANQUANTES ===")

result = sql("""
SELECT count(*)
FROM lakehouse.gold.fact_orders
WHERE user_key IS NULL
   OR product_key IS NULL
   OR date_key IS NULL
""")

print("Nombre de commandes avec clé manquante :", result[0][0])

conn.close()