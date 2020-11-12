from pyspark import SparkConf
from pyspark.sql import SparkSession
import argparse as arg


def fib():
    return


def isPrime(n: int) -> bool:
    if n <= 1:
        return False
    for i in range(2, n):
        if n % i == 0:
            return False
    return True


def arg_parser():
    try:
        parser = arg.ArgumentParser(
            description="API SPARK CODE - Parsing JSON Weather data",
            usage="%(prog)s parameters"
        )
        parser.add_argument(
            "-f",
            "--input-file",
            required=True,
            dest="input_file"
        )
        a = parser.parse_args()
        return {"input_file": a.input_file}

    except Exception as ex:
        print(ex)
        return {}


if __name__ == "__main__":

    d = arg_parser()

    if 'input_file' in d:

        conf = SparkConf().setAppName("APIWeather-Job")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # print(spark.version)
        x = spark.sparkContext.parallelize([i for i in range(10000)])
        s = x.filter(isPrime).count()
        print(s)

    else:
        print("Cannot locate input file")
