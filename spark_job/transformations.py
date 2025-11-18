from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, regexp_replace
)
from pyspark.sql.types import IntegerType, FloatType


def normalize_product_names(df: DataFrame) -> DataFrame:
    """
    Clean and normalize product names:
    - Trim spaces
    - Convert to uppercase
    - Replace null/NA/empty with 'UNKNOWN'
    """
    return (
        df.withColumn("product", trim(upper(col("product"))))
          .withColumn(
              "product",
              when(
                  (col("product").isNull()) |
                  (col("product") == "") |
                  (col("product") == "NA") |
                  (col("product") == "NULL"),
                  "UNKNOWN"
              ).otherwise(col("product"))
          )
    )


def fix_quantity(df: DataFrame) -> DataFrame:
    """
    Clean 'quantity' column:
    - Replace textual numbers ('one', 'three') with digits
    - Replace invalid or missing values with 0
    - Convert final quantity to integer
    """

    df = df.withColumn("quantity", trim(col("quantity")))

    replacements = {
        "one": "1",
        "three": "3",
        "two": "2",
        "five": "5",
        "four": "4"
    }

    for word, num in replacements.items():
        df = df.withColumn(
            "quantity",
            when(upper(col("quantity")) == word.upper(), num)
            .otherwise(col("quantity"))
        )

    df = df.withColumn(
        "quantity",
        when(col("quantity").rlike("^[0-9]+$"), col("quantity"))
        .otherwise("0")
    )

    return df.withColumn("quantity", col("quantity").cast(IntegerType()))


def fix_price(df: DataFrame) -> DataFrame:
    """
    Clean 'price' column:
    - Remove extra spaces
    - Convert invalid values to 0
    - Cast to float
    """

    df = df.withColumn("price", trim(col("price")))

    return (
        df.withColumn(
            "price",
            when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price"))
            .otherwise("0")
        )
        .withColumn("price", col("price").cast(FloatType()))
    )


def add_revenue(df: DataFrame) -> DataFrame:
    """Create revenue column = quantity * price."""
    return df.withColumn("revenue", col("quantity") * col("price"))


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate rows based on order_id."""
    return df.dropDuplicates(["order_id"])


def clean_data(df: DataFrame) -> DataFrame:
    """
    Full cleaning pipeline (used inside job.py):
    1. Normalize product names
    2. Fix quantity
    3. Fix price
    4. Remove duplicates
    5. Add revenue
    """
    df = normalize_product_names(df)
    df = fix_quantity(df)
    df = fix_price(df)
    df = remove_duplicates(df)
    df = add_revenue(df)
    return df