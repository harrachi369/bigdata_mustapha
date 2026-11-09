import os
from pyspark.sql import SparkSession, functions as F

BUCKET = os.environ["KUBERNETES_NAMESPACE"]

POLARIS_URI = "http://polaris:8181"
POLARIS_CATALOG = BUCKET

spark = (
    SparkSession.builder
    .appName("lab3-spark-vs-trino")
    .master("local[2]")

    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")

    # Accès S3
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"https://{os.environ['AWS_S3_ENDPOINT']}"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["AWS_ACCESS_KEY_ID"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    .config(
        "spark.hadoop.fs.s3a.session.token",
        os.environ.get("AWS_SESSION_TOKEN", "")
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # Iceberg
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Catalogue Polaris
    .config(
        "spark.sql.catalog.polaris",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.polaris.type", "rest")
    .config(
        "spark.sql.catalog.polaris.uri",
        f"{POLARIS_URI}/api/catalog"
    )
    .config(
        "spark.sql.catalog.polaris.warehouse",
        POLARIS_CATALOG
    )
    .config(
        "spark.sql.catalog.polaris.credential",
        os.environ["POLARIS_DEV_CREDENTIAL"]
    )
    .config(
        "spark.sql.catalog.polaris.scope",
        "PRINCIPAL_ROLE:ALL"
    )
    .config(
        "spark.sql.catalog.polaris.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# 1. Construire l'agrégation avec Spark
spark_result = spark.sql("""
SELECT
    u.state,
    date_trunc('month', o.ordered_at) AS month,
    count(*) AS orders,
    sum(o.quantity) AS units,
    count(DISTINCT o.user_id) AS buyers
FROM polaris.silver.orders o
JOIN polaris.silver.users u
    ON o.user_id = u.user_id
GROUP BY 1, 2
""")


# 2. Lire la table construite avec Trino
trino_result = spark.table(
    "polaris.gold.orders_by_state_month"
)


# 3. Comparer les nombres de lignes
spark_count = spark_result.count()
trino_count = trino_result.count()

print("\n=== COMPARAISON SPARK / TRINO ===")
print("Lignes Spark :", spark_count)
print("Lignes Trino :", trino_count)


# 4. Comparer les totaux
spark_totals = spark_result.agg(
    F.sum("orders").alias("orders"),
    F.sum("units").alias("units")
).first()

trino_totals = trino_result.agg(
    F.sum("orders").alias("orders"),
    F.sum("units").alias("units")
).first()

print("\n=== TOTAUX SPARK ===")
print(spark_totals)

print("\n=== TOTAUX TRINO ===")
print(trino_totals)


# 5. Assertions
assert spark_count == trino_count

assert spark_totals["orders"] == trino_totals["orders"]
assert spark_totals["units"] == trino_totals["units"]

print("\n✅ RESULTATS SPARK ET TRINO COHERENTS")

spark.stop()