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


# 1. Analyse des ventes par produit, État et année
print("\n=== VENTES PAR PRODUIT, ETAT ET ANNEE ===")

result = sql("""
SELECT
    d.year,
    u.state,
    p.product,
    count(*) AS orders,
    sum(f.quantity) AS units
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_date d
    ON f.date_key = d.date_key
JOIN lakehouse.gold.dim_user u
    ON f.user_key = u.user_key
JOIN lakehouse.gold.dim_product p
    ON f.product_key = p.product_key
GROUP BY
    d.year,
    u.state,
    p.product
ORDER BY units DESC
LIMIT 10
""")

print(result.to_string(index=False))


# 2. Analyse des commandes par État et par mois
print("\n=== COMMANDES PAR ETAT ET PAR MOIS ===")

result = sql("""
SELECT
    u.state,
    date_trunc('month', d.full_date) AS month,
    count(*) AS orders,
    sum(f.quantity) AS units,
    count(DISTINCT f.user_key) AS buyers
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_date d
    ON f.date_key = d.date_key
JOIN lakehouse.gold.dim_user u
    ON f.user_key = u.user_key
GROUP BY
    u.state,
    date_trunc('month', d.full_date)
ORDER BY month, state
LIMIT 10
""")

print(result.to_string(index=False))

conn.close()