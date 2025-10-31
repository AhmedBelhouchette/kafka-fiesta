from pyspark.sql import SparkSession
import argparse

from src.spark_jobs.common.spark_session import get_spark_session


def main(input_path: str):
    spark = get_spark_session("energy_monitor")
    # Placeholder: logique d'agrégation des consommations
    print(f"Energy monitor job would process: {input_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/energy_samples.csv")
    args = parser.parse_args()
    main(args.input)