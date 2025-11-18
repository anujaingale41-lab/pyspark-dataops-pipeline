from pyspark.sql import SparkSession
from spark_job.job import run_spark_job

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PySparkDataOpsPipeline") \
        .master("local[*]") \
        .getOrCreate()

    # Input & output paths
    input_path = "data/sales_raw.csv"
    output_path = "data/clean/sales_clean.csv"

    print("Starting PySpark job...")
    run_spark_job(spark, input_path, output_path)
    print("Job completed. Cleaned file saved at:", output_path)

    # Stop Spark session
    spark.stop()