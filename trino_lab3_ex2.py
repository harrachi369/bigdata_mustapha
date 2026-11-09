import os
import pandas as pd
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
    columns = [col[0] for col in cursor.description]

    return pd.DataFrame(
        cursor.fetchall(),
        columns=columns
    )


# 1. Créer la table des cohortes
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.cohorts AS
SELECT
    CAST(
        floor(year(u.birthdate) / 10.0) * 10
        AS integer
    ) AS birth_decade,

    count(DISTINCT u.user_key) AS users,

    count(f.order_id) AS orders,

    round(
        avg(CAST(f.quantity AS double)),
        2
    ) AS avg_units_per_order

FROM lakehouse.gold.dim_user u

LEFT JOIN lakehouse.gold.fact_orders f
    ON u.user_key = f.user_key

GROUP BY 1
""")


# 2. Afficher les cohortes
print("\n=== ANALYSE DES COHORTES ===")

result = sql("""
SELECT *
FROM lakehouse.gold.cohorts
ORDER BY birth_decade
""")

print(result.to_string(index=False))


# 3. Vérifier les totaux
print("\n=== VALIDATION DES COHORTES ===")

result = sql("""
SELECT
    sum(users) AS total_users,
    sum(orders) AS total_orders
FROM lakehouse.gold.cohorts
""")

print(result.to_string(index=False))

assert int(result.iloc[0]["total_users"]) == 50
assert int(result.iloc[0]["total_orders"]) == 2829

print("\n✅ VALIDATION DES COHORTES REUSSIE")

conn.close()