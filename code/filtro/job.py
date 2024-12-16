from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn

spark = SparkSession.builder.appName("desenvolvimento-teste-eng-dados").getOrCreate()

INPUT_SCHEMA = "nu_contrato bigint, nu_controle bigint, valor decimal(10,2), data date"


def filtra_data_menor_ou_igual_01_01_2025(df: DataFrame):
    return df.filter("data <= '2025-01-01'")


def filtra_maior_nu_controle(df: DataFrame):
    df_com_max_controle = df.groupBy("nu_contrato").agg(
        fn.max("nu_controle").alias("nu_controle")
    )
    return df.join(df_com_max_controle, ["nu_contrato", "nu_controle"])


def filtra_data_e_controle(df: DataFrame):
    return df.transform(filtra_data_menor_ou_igual_01_01_2025).transform(
        filtra_maior_nu_controle
    )


if __name__ == "__main__":
    (
        spark.read.csv(
            str(Path(__file__).parent / "data.csv"), header=True, schema=INPUT_SCHEMA
        )
        .transform(filtra_data_e_controle)
        .write.csv("/tmp/filtro", header=True, mode="overwrite")
    )
