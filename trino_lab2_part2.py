import os
import pandas as pd
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
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def show(title, query):
    print(f"\n=== {title} ===")
    result = sql(query)
    print(result.to_string(index=False))


# 1. Snapshots Iceberg
show(
    "SNAPSHOTS ORDERS",
    """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
    """
)


# 2. Fichiers physiques
show(
    "FICHIERS PARQUET ORDERS",
    """
    SELECT file_path, file_format,
           record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files"
    """
)


# 3. Historique des snapshots
show(
    "HISTORIQUE ORDERS",
    """
    SELECT *
    FROM lakehouse.silver."orders$history"
    """
)

conn.close()