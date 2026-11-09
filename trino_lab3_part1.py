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


# 1. Dimension utilisateurs
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user AS
SELECT
    row_number() OVER (ORDER BY user_id) AS user_key,
    user_id,
    username,
    name,
    sex,
    birthdate,
    city,
    state,
    zip_code
FROM lakehouse.silver.users
""")

print("dim_user : créée")


# 2. Dimension produits
sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product AS
SELECT
    row_number() OVER (ORDER BY product) AS product_key,
    product
FROM (
    SELECT DISTINCT product
    FROM lakehouse.silver.orders
)
""")

print("dim_product : créée")


# 3. Récupérer la période des commandes
first_day, last_day = sql("""
SELECT
    CAST(min(ordered_at) AS date),
    CAST(max(ordered_at) AS date)
FROM lakehouse.silver.orders
""")[0]

print("Première date :", first_day)
print("Dernière date :", last_day)


# 4. Dimension dates
sql(f"""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date AS
SELECT
    CAST(
        date_format(CAST(d AS timestamp), '%Y%m%d')
        AS integer
    ) AS date_key,
    d AS full_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    date_format(CAST(d AS timestamp), '%M') AS month_name,
    day_of_week(d) AS day_of_week,
    day_of_week(d) IN (6, 7) AS is_weekend
FROM UNNEST(
    sequence(
        DATE '{first_day}',
        DATE '{last_day}',
        INTERVAL '1' DAY
    )
) AS t(d)
""")

print("dim_date : créée")


# 5. Vérifier le nombre de lignes
print("\n=== DIMENSIONS CREEES ===")

results = sql("""
SELECT 'dim_user' AS table_name, count(*) AS rows
FROM lakehouse.gold.dim_user

UNION ALL

SELECT 'dim_product', count(*)
FROM lakehouse.gold.dim_product

UNION ALL

SELECT 'dim_date', count(*)
FROM lakehouse.gold.dim_date
""")

for row in results:
    print(row)

conn.close()