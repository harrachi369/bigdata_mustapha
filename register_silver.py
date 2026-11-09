import os
from pyspark.sql import SparkSession

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
POLARIS_URI = "http://polaris:8181"

spark = (
    SparkSession.builder
    .appName("register-silver-polaris")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"https://{os.environ['AWS_S3_ENDPOINT']}"
    )
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ["AWS_SESSION_TOKEN"])
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", BUCKET)
    .config(
        "spark.sql.catalog.polaris.credential",
        os.environ["POLARIS_DEV_CREDENTIAL"]
    )
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config(
        "spark.sql.catalog.polaris.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Creer le namespace Silver dans Polaris
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

# Enregistrer les deux tables Iceberg existantes
existing = {
    row.tableName
    for row in spark.sql("SHOW TABLES IN polaris.silver").collect()
}

for table in ("orders", "users"):
    if table not in existing:
        metadata = (
            f"s3a://{BUCKET}/warehouse/silver/"
            f"{table}/metadata/v1.metadata.json"
        )

        spark.sql(f"""
            CALL polaris.system.register_table(
                table => 'silver.{table}',
                metadata_file => '{metadata}'
            )
        """).show()

# Verifications
print("=== TABLES POLARIS ===")
spark.sql("SHOW TABLES IN polaris.silver").show()

print("=== NOMBRE DE LIGNES ===")
spark.sql("""
    SELECT 'users' AS table_name, count(*) AS rows
    FROM polaris.silver.users

    UNION ALL

    SELECT 'orders', count(*)
    FROM polaris.silver.orders
""").show()

spark.stop()