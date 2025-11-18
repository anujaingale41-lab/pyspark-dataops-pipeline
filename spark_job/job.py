from pyspark.sql import SparkSession
from spark_job.transformations import clean_sales_data


def main():
    # 1. Create Spark session
    spark = SparkSession.builder \
        .appName("PySpark DataOps Pipeline") \
        .getOrCreate()

    # 2. Read raw CSV
    input_path = "data/sales_raw.csv"
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    # 3. Apply transformations
    df_clean = clean_sales_data(df_raw)

    # 4. Write output
    output_path = "output/sales_clean"
    df_clean.write.mode("overwrite").csv(output_path, header=True)

    print(f"Job completed. Cleaned data saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()