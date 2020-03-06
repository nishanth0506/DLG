import pytest
from pyspark.sql import Row

from solution.solution import find_hottest_day
from solution.solution import find_hottest_day_temp
from solution.solution import find_hottest_day_region


@pytest.mark.usefixtures("spark")
def test_find_hottest_day_temp(spark):
    weather_df = spark.createDataFrame([
        Row(ScreenTemperature=float(1.3), ObservationDate="2016-02-21T00:00:00", Region="Shetland"),
        Row(ScreenTemperature=float(4.2), ObservationDate="2016-02-22T00:00:00", Region="Strathclyde"),
        Row(ScreenTemperature=float(9.3), ObservationDate="2016-02-23T00:00:00", Region="Orkney & Shetland"),
        Row(ScreenTemperature=float(5.3), ObservationDate="2016-02-24T00:00:00", Region="Highland & Eilean Siar")
    ])
    expected = float(9.3)
    actual = find_hottest_day_temp(weather_df)
    assert actual == expected


@pytest.mark.usefixtures("spark")
def test_find_hottest_day(spark):
    weather_df = spark.createDataFrame([
        Row(ScreenTemperature=float(1.3), ObservationDate="2016-02-21T00:00:00", Region="Shetland"),
        Row(ScreenTemperature=float(4.2), ObservationDate="2016-02-22T00:00:00", Region="Strathclyde"),
        Row(ScreenTemperature=float(9.3), ObservationDate="2016-02-23T00:00:00", Region="Orkney & Shetland"),
        Row(ScreenTemperature=float(5.3), ObservationDate="2016-02-24T00:00:00", Region="Highland & Eilean Siar")
    ])
    expected = "2016-02-23T00:00:00"
    actual = find_hottest_day(weather_df)
    assert actual == expected


@pytest.mark.usefixtures("spark")
def test_find_hottest_day_region(spark):
    weather_df = spark.createDataFrame([
        Row(ScreenTemperature=float(1.3), ObservationDate="2016-02-21T00:00:00", Region="Shetland"),
        Row(ScreenTemperature=float(4.2), ObservationDate="2016-02-22T00:00:00", Region="Strathclyde"),
        Row(ScreenTemperature=float(9.3), ObservationDate="2016-02-23T00:00:00", Region="Orkney & Shetland"),
        Row(ScreenTemperature=float(5.3), ObservationDate="2016-02-24T00:00:00", Region="Highland & Eilean Siar")
    ])
    expected = "Orkney & Shetland"
    actual = find_hottest_day_region(weather_df)
    assert actual == expected
