from pyspark.sql import SparkSession
import argparse
from src.spark_jobs.common.spark_session import get_spark_session

def main(input_path: str):
    spark = get_spark_session("maintenance_predictor")
    # Placeholder: lire les données et exécuter pipeline ML
    print(f"Would run maintenance prediction on: {input_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/sensor_samples.csv")
    args = parser.parse_args()
    main(args.input)