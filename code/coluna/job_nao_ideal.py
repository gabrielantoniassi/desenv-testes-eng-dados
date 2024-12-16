from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql import types as tp

spark = SparkSession.builder.appName("desenvolvimento-teste-eng-dados").getOrCreate()

INPUT_SCHEMA = (
    "nu_contrato bigint,"
    "tipo string,"
    "valor decimal(10,2),"
    "taxa_mensal decimal(4,3),"
    "nu_parcelas int,"
    "nu_parcelas_pagas int"
)


if __name__ == "__main__":
    df = spark.read.csv(
        str(Path(__file__).parent / "data.csv"), header=True, schema=INPUT_SCHEMA
    )

    df = df.withColumn(
        "saldo_devedor",
        fn.when(
            fn.col("tipo") == "SAC",
            fn.col("valor")
            / fn.col("nu_parcelas")
            * (fn.col("nu_parcelas") - fn.col("nu_parcelas_pagas")),
        )
        .otherwise(
            fn.col("valor")
            * (
                fn.pow(1 + fn.col("taxa_mensal"), fn.col("nu_parcelas"))
                - fn.pow(1 + fn.col("taxa_mensal"), fn.col("nu_parcelas_pagas"))
            )
            / (fn.pow(1 + fn.col("taxa_mensal"), fn.col("nu_parcelas")) - 1)
        )
        .cast(tp.DecimalType(10, 2)),
    )
    df = df.select("nu_contrato", "saldo_devedor")

    df.write.csv("/tmp/coluna", header=True, mode="overwrite")
