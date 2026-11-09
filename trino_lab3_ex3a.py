import os
import trino

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user=os.environ["KUBERNETES_NAMESPACE"],
    catalog="lakehouse",
)


def execute(query):
    cursor = conn.cursor()
    cursor.execute(query)


def sql(query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


ORDER_ID = "lab3-ex3-mustapha-001"


# 1. Vérifier l'état initial
print("\n=== AVANT INSERTION ===")

before = sql("""
SELECT count(*), sum(quantity)
FROM lakehouse.silver.orders
""")[0]

print("Silver :", before)


# 2. Vérifier que la commande de test n'existe pas
existing = sql(f"""
SELECT count(*)
FROM lakehouse.silver.orders
WHERE order_id = '{ORDER_ID}'
""")[0][0]


# 3. Ajouter une seule commande
if existing == 0:

    execute(f"""
    INSERT INTO lakehouse.silver.orders
        (order_id, user_id, ordered_at, quantity, product)

    SELECT
        '{ORDER_ID}',
        user_id,
        ordered_at,
        2,
        product
    FROM lakehouse.silver.orders
    ORDER BY order_id
    LIMIT 1
    """)

    print("\nNouvelle commande insérée.")

else:
    print("\nLa commande de test existe déjà.")


# 4. Vérifier la commande
print("\n=== COMMANDE AJOUTEE ===")

result = sql(f"""
SELECT *
FROM lakehouse.silver.orders
WHERE order_id = '{ORDER_ID}'
""")

for row in result:
    print(row)


# 5. Vérifier les nouveaux totaux
print("\n=== APRES INSERTION ===")

after = sql("""
SELECT count(*), sum(quantity)
FROM lakehouse.silver.orders
""")[0]

print("Silver :", after)

assert tuple(after) == (2830, 8617)

print("\n✅ INSERTION SILVER REUSSIE")

conn.close()