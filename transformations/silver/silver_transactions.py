from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, lower, date_format, regexp_replace


@dp.materialized_view(
    name="silver_transactions",
    comment="Silver layer: cleaned and validated transactions"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL AND trim(transaction_id) != ''")
@dp.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_unit_price", "unit_price > 0")
@dp.expect_or_drop("valid_total_amount", "total_amount > 0")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL AND trim(customer_id) != ''")
def silver_transactions():
    return spark.read.table("bronze_transactions") \
        .withColumns({
            "transaction_id": trim(col("transaction_id")),
            "customer_id": trim(col("customer_id")),
            "product_id": trim(col("product_id")),
            "product_name": regexp_replace(lower(trim(col("product_name"))), "\\s+", " "),
            "category": lower(trim(col("category"))),
            "store_location": trim(col("store_location")),
            "payment_method": lower(trim(col("payment_method"))),
            "transaction_date_only": date_format(col("transaction_date"), "yyyy-MM-dd"),
        })
