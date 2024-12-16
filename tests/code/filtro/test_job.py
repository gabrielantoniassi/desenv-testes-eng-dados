from code.filtro.job import (
    INPUT_SCHEMA,
    filtra_data_e_controle,
    filtra_data_menor_ou_igual_01_01_2025,
    filtra_maior_nu_controle,
)
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession


class TestFiltraDataMenorOuIgual01012025:
    def act_e_assert(self, input_df: DataFrame, expected_df: DataFrame):
        """Método que recebe o DataFrame de entrada e o esperado
        e realiza as etapas de Act e Assert"""
        output_df = input_df.transform(filtra_data_menor_ou_igual_01_01_2025)

        assert output_df.dtypes == expected_df.dtypes
        assert output_df.count() == expected_df.count()
        assert output_df.collect() == expected_df.collect()

    def test_quando_data_anterior_a_01_01_2025_manter(self, spark: SparkSession):

        input_df = spark.createDataFrame(
            [(1, 1, Decimal("10.00"), date(2024, 1, 1))], INPUT_SCHEMA
        )
        expected_df = input_df

        self.act_e_assert(input_df, expected_df)

    def test_quando_data_igual_a_01_01_2025_manter(self, spark: SparkSession):

        input_df = spark.createDataFrame(
            [(1, 1, Decimal("10.00"), date(2025, 1, 1))], INPUT_SCHEMA
        )
        expected_df = input_df

        self.act_e_assert(input_df, expected_df)

    def test_quando_data_posterior_a_01_01_2025_remover(self, spark: SparkSession):

        input_df = spark.createDataFrame(
            [(1, 1, Decimal("10.00"), date(2025, 1, 2))], INPUT_SCHEMA
        )
        expected_df = spark.createDataFrame([], INPUT_SCHEMA)

        self.act_e_assert(input_df, expected_df)


class TestFiltraMaiorNuControle:
    def act_e_assert(self, input_df: DataFrame, expected_df: DataFrame):
        """Método que recebe o DataFrame de entrada e o esperado
        e realiza as etapas de Act e Assert"""
        output_df = input_df.transform(filtra_maior_nu_controle)

        assert output_df.dtypes == expected_df.dtypes
        assert output_df.count() == expected_df.count()
        assert output_df.collect() == expected_df.collect()

    def test_quando_nu_contrato_unico_mater(self, spark: SparkSession):
        input_df = spark.createDataFrame(
            [(1, 1, Decimal("10.00"), date(2024, 1, 1))], INPUT_SCHEMA
        )
        expected_df = input_df

        self.act_e_assert(input_df, expected_df)

    def test_quando_nu_contrato_nao_unico_manter_maior(self, spark: SparkSession):
        input_df = spark.createDataFrame(
            [
                (1, 1, Decimal("10.00"), date(2024, 1, 1)),
                (1, 2, Decimal("20.00"), date(2024, 1, 1)),
            ],
            INPUT_SCHEMA,
        )
        expected_df = spark.createDataFrame(
            [(1, 2, Decimal("20.00"), date(2024, 1, 1))], INPUT_SCHEMA
        )

        self.act_e_assert(input_df, expected_df)


def test_filtra_datas_e_controle(spark: SparkSession):
    input_df = spark.createDataFrame(
        [
            (1, 1, Decimal("10.00"), date(2024, 1, 1)),
            (1, 2, Decimal("20.00"), date(2024, 6, 1)),
            (1, 3, Decimal("30.00"), date(2025, 2, 1)),
        ],
        INPUT_SCHEMA,
    )
    expected_df = spark.createDataFrame(
        [(1, 2, Decimal("20.00"), date(2024, 6, 1))], INPUT_SCHEMA
    )

    output_df = input_df.transform(filtra_data_e_controle)

    assert output_df.dtypes == expected_df.dtypes
    assert output_df.count() == expected_df.count()
    assert output_df.collect() == expected_df.collect()
