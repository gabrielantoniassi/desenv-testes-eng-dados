from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql import types as tp

spark = SparkSession.builder.appName("desenvolvimento-teste-eng-dados").getOrCreate()

INPUT_SCHEMA = "nu_contrato bigint, nu_controle bigint, valor decimal(10,2), data date"


if __name__ == "__main__":
    df = spark.read.csv(
        str(Path(__file__).parent / "data.csv"), header=True, schema=INPUT_SCHEMA
    )

    df_com_max_controle = (
        df.filter("data <= '2025-01-01'")
        .groupBy("nu_contrato")
        .agg(fn.max("nu_controle").alias("nu_controle"))
    )
    df = df.join(df_com_max_controle, ["nu_contrato", "nu_controle"])

    df.write.csv("/tmp/filtro", header=True, mode="overwrite")
