from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count, countDistinct, round as _round


@dp.materialized_view(
    name="gold_daily_transactions",
    comment="Gold layer: daily transaction summary with totals and counts",
    cluster_by=["transaction_date"]
)
def gold_daily_transactions():
    return spark.read.table("silver_transactions") \
        .groupBy(to_date(col("transaction_date")).alias("transaction_date")) \
        .agg(
            _count("transaction_id").alias("transaction_count"),
            _round(_sum("total_amount"), 2).alias("total_revenue"),
            _sum("quantity").alias("total_quantity"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        ) \
        .orderBy(col("transaction_date"))
