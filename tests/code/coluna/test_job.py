from code.coluna.job import INPUT_SCHEMA, saldo_devedor
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession


class TestSaldoDevedor:
    expected_schema = "nu_contrato bigint, saldo_devedor decimal(10, 2)"

    def act_e_assert(self, input_df: DataFrame, expected_df: DataFrame):
        """Método que recebe o DataFrame de entrada e o esperado
        e realiza as etapas de Act e Assert"""
        output_df = input_df.select("nu_contrato", saldo_devedor)

        assert output_df.dtypes == expected_df.dtypes
        assert output_df.collect() == expected_df.collect()

    def test_quando_contrato_sac(self, spark: SparkSession):
        input_df = spark.createDataFrame(
            [
                (1, "SAC", Decimal("1000.00"), Decimal("0.030"), 4, 3),
                (2, "SAC", Decimal("260000.00"), Decimal("0.015"), 100, 30),
            ],
            INPUT_SCHEMA,
        )
        expected_df = spark.createDataFrame(
            [(1, Decimal("250.00")), (2, Decimal("182000.00"))],
            "nu_contrato bigint, saldo_devedor decimal(10, 2)",
        )

        self.act_e_assert(input_df, expected_df)

    def test_quando_contrato_price(self, spark: SparkSession):
        input_df = spark.createDataFrame(
            [
                (3, "PRICE", Decimal("2000.00"), Decimal("0.030"), 4, 1),
                (4, "PRICE", Decimal("180000.00"), Decimal("0.020"), 200, 150),
            ],
            INPUT_SCHEMA,
        )
        expected_df = spark.createDataFrame(
            [(3, Decimal("1521.95")), (4, Decimal("115322.23"))],
            "nu_contrato bigint, saldo_devedor decimal(10, 2)",
        )

        self.act_e_assert(input_df, expected_df)
