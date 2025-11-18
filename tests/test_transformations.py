import pytest
from pyspark.sql import SparkSession
from spark_job.transformations import clean_sales_data


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_clean_sales_data(spark):
    # Sample raw data (with bad values)
    sample_data = [
        ("1", "A", "2", "100"),
        ("", "B", "1", "200"),        # missing order_id → should drop
        ("3", "C", None, "150"),      # null quantity → fill with 0
        ("4", "D", "x", "250"),       # bad quantity → cast becomes null → fill with 0
    ]

    columns = ["order_id", "product", "quantity", "price"]

    df_raw = spark.createDataFrame(sample_data, columns)

    df_clean = clean_sales_data(df_raw)

    # Collect results
    result = df_clean.collect()

    # 1. Row with missing order_id should be removed
    assert not any(r.order_id == "" for r in result)

    # 2. Null or invalid quantity should become 0
    for r in result:
        assert r.quantity is not None

    # 3. Should not drop valid rows
    assert len(result) == 3
