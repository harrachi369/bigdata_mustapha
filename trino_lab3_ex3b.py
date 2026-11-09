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


def execute(query):
    cursor = conn.cursor()
    cursor.execute(query)


# 1. Enregistrer le snapshot initial
print("\n=== SNAPSHOTS AVANT MERGE ===")

snapshots_before = sql("""
SELECT snapshot_id, committed_at, operation
FROM lakehouse.gold."orders_by_state_month$snapshots"
ORDER BY committed_at
""")

for row in snapshots_before:
    print(row)

first_snapshot = snapshots_before[0][0]


# 2. Comparer Silver et Gold avant MERGE
print("\n=== TOTAUX AVANT MERGE ===")

silver = sql("""
SELECT count(*), sum(quantity)
FROM lakehouse.silver.orders
""")[0]

gold_before = sql("""
SELECT sum(orders), sum(units)
FROM lakehouse.gold.orders_by_state_month
""")[0]

print("Silver :", silver)
print("Gold :", gold_before)

assert tuple(silver) == (2830, 8617)


# 3. Actualiser Gold
if tuple(gold_before) == (2829, 8615):

    execute("""
    MERGE INTO lakehouse.gold.orders_by_state_month AS t

    USING (
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
    ) AS s

    ON t.state = s.state
       AND t.month = s.month

    WHEN MATCHED AND (
        t.orders <> s.orders
        OR t.units <> s.units
        OR t.buyers <> s.buyers
    )
    THEN UPDATE SET
        orders = s.orders,
        units = s.units,
        buyers = s.buyers

    WHEN NOT MATCHED
    THEN INSERT (state, month, orders, units, buyers)
    VALUES (s.state, s.month, s.orders, s.units, s.buyers)
    """)

    print("\nMERGE : terminé")

elif tuple(gold_before) == tuple(silver):
    print("\nGold est déjà synchronisé : MERGE ignoré.")

else:
    raise RuntimeError(
        "Totaux inattendus : vérification nécessaire."
    )


# 4. Vérifier les nouveaux totaux
print("\n=== TOTAUX APRES MERGE ===")

gold_after = sql("""
SELECT sum(orders), sum(units)
FROM lakehouse.gold.orders_by_state_month
""")[0]

print("Gold actuel :", gold_after)

assert tuple(gold_after) == (2830, 8617)


# 5. Consulter les snapshots après MERGE
print("\n=== SNAPSHOTS APRES MERGE ===")

snapshots_after = sql("""
SELECT snapshot_id, committed_at, operation
FROM lakehouse.gold."orders_by_state_month$snapshots"
ORDER BY committed_at
""")

for row in snapshots_after:
    print(row)


# 6. Time Travel vers la première version
print("\n=== TIME TRAVEL ===")

old_version = sql(f"""
SELECT sum(orders), sum(units)
FROM lakehouse.gold.orders_by_state_month
FOR VERSION AS OF {first_snapshot}
""")[0]

print("Version initiale :", old_version)
print("Version actuelle :", gold_after)

assert tuple(old_version) == (2829, 8615)

print("\n✅ MERGE ET TIME TRAVEL REUSSIS")

conn.close()