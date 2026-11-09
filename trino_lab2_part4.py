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
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [col[0] for col in cursor.description]
    print(" | ".join(columns))

    for row in cursor.fetchall():
        print(" | ".join(str(value) for value in row))


# 1. Créer le schéma Gold
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
print("Schéma Gold : OK")


# 2. Commandes par État et par mois
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month AS
SELECT
    u.state,
    date_trunc('month', o.ordered_at) AS month,
    count(*) AS orders,
    sum(o.quantity) AS units,
    count(DISTINCT o.user_id) AS buyers
FROM lakehouse.silver.orders o
JOIN lakehouse.silver.users u
    ON u.user_id = o.user_id
GROUP BY 1, 2
""")

show(
    "ORDERS BY STATE MONTH",
    """
    SELECT *
    FROM lakehouse.gold.orders_by_state_month
    ORDER BY month, state
    LIMIT 10
    """
)


# 3. Classement des produits
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products AS
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

show(
    "TOP PRODUCTS",
    """
    SELECT *
    FROM lakehouse.gold.top_products
    ORDER BY units_rank
    LIMIT 10
    """
)


# 4. Activité des utilisateurs
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.user_activity AS
SELECT
    u.user_id,
    u.state,
    date_diff('year', u.birthdate, current_date) AS age,
    count(o.order_id) AS orders,
    coalesce(sum(o.quantity), 0) AS units,
    min(o.ordered_at) AS first_order_at,
    max(o.ordered_at) AS last_order_at
FROM lakehouse.silver.users u
LEFT JOIN lakehouse.silver.orders o
    ON o.user_id = u.user_id
GROUP BY u.user_id, u.state, u.birthdate
""")

show(
    "USER ACTIVITY",
    """
    SELECT *
    FROM lakehouse.gold.user_activity
    ORDER BY orders DESC
    LIMIT 10
    """
)


# 5. Vérifier les tables créées
show(
    "TABLES GOLD",
    "SHOW TABLES FROM lakehouse.gold"
)


# 6. Vérifier les fichiers Iceberg
show(
    "FICHIERS TOP PRODUCTS",
    """
    SELECT
        file_path,
        record_count,
        file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
    """
)

conn.close()