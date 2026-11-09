import os
import pandas as pd
import trino

CATALOG = "lakehouse"

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user=os.environ["KUBERNETES_NAMESPACE"],
    http_scheme="http",
    catalog=CATALOG,
)

def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)

def show(title, statement):
    print(f"\n=== {title} ===")
    print(sql(statement).to_string(index=False))


# 1. Connexion
show("VERSION TRINO", "SELECT version()")

# 2. Catalogues
show("CATALOGUES", "SHOW CATALOGS")

# 3. Schemas
show("SCHEMAS", "SHOW SCHEMAS FROM lakehouse")

# 4. Tables Silver
show("TABLES SILVER", "SHOW TABLES FROM lakehouse.silver")

# 5. Structure des tables
show("STRUCTURE ORDERS", "DESCRIBE lakehouse.silver.orders")

show("STRUCTURE USERS", "DESCRIBE lakehouse.silver.users")

# 6. Nombre de lignes
show(
    "NOMBRE DE COMMANDES",
    "SELECT count(*) AS orders FROM lakehouse.silver.orders"
)

show(
    "NOMBRE D'UTILISATEURS",
    "SELECT count(*) AS users FROM lakehouse.silver.users"
)

# 7. Verification des adresses
show("UTILISATEURS PAR ETAT", """
    SELECT
        state,
        count(*) AS users,
        count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
""")

# 8. Verification des commandes orphelines
show("COMMANDES ORPHELINES", """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u
        ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")

conn.close()