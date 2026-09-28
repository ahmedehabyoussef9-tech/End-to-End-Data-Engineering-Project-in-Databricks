from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.materialized_view(
    name="bronze_transactions",
    comment="Bronze layer: raw ingestion from dataengineering.endtoend.transactions"
)
def bronze_transactions():
    return spark.read.table("dataengineering.endtoend.transactions") \
        .withColumn("ingestion_timestamp", current_timestamp())
