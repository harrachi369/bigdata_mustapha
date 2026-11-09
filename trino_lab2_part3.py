import os
import trino

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user=os.environ["KUBERNETES_NAMESPACE"],
    catalog="lakehouse",
    schema="silver",
)


def sql(query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


query = """
SELECT u.state, count(*) AS orders
FROM lakehouse.silver.orders AS o
JOIN lakehouse.silver.users AS u
    ON u.user_id = o.user_id
GROUP BY u.state
"""


# 1. Plan d'exécution
print("\n=== EXPLAIN DISTRIBUTED ===")

plan = sql(f"EXPLAIN (TYPE DISTRIBUTED) {query}")

print(plan[0][0])


# 2. Exécution réelle
print("\n=== EXPLAIN ANALYZE ===")

stats = sql(f"EXPLAIN ANALYZE {query}")

print(stats[0][0])


# 3. Historique des requêtes Trino
print("\n=== DERNIERES REQUETES ===")

results = sql("""
SELECT
    query_id,
    state,
    date_diff(
        'millisecond',
        created,
        coalesce("end", current_timestamp)
    ) AS elapsed_ms,
    query
FROM system.runtime.queries
ORDER BY created DESC
LIMIT 5
""")

for row in results:
    print(row)

conn.close()