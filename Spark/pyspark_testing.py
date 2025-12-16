import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, lit, avg, udf
from pyspark.sql.types import StringType
import pygeohash as pgh

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("spark_unit_test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()



@udf(StringType())
def geohash_4(lat, lng):
    try:
        if lat is None or lng is None:
            return None
        return pgh.encode(float(lat), float(lng))[:4]
    except Exception:
        return None


def test_geohash_udf_valid(spark):
    # Normal case
    df = spark.createDataFrame([Row(lat=37.7749, lng=-122.4194)])
    df = df.withColumn("hash", geohash_4("lat", "lng"))
    result = df.collect()[0]["hash"]
    assert len(result) == 4
    assert isinstance(result, str)

def test_geohash_udf_none(spark):
    # Edge case: missing lat/lng
    df = spark.createDataFrame([Row(lat=None, lng=None)])
    df = df.withColumn("hash", geohash_4("lat", "lng"))
    result = df.collect()[0]["hash"]
    assert result is None

def test_fill_missing_coordinates(spark):
    # Test filling missing lat/lng
    df = spark.createDataFrame([
        Row(lat=None, lng=None),
        Row(lat=1.0, lng=2.0)
    ])
    new_lat, new_lng = 10.0, 20.0
    df_fixed = df.withColumn("lat", when(col("lat").isNull(), lit(new_lat)).otherwise(col("lat"))) \
                 .withColumn("lng", when(col("lng").isNull(), lit(new_lng)).otherwise(col("lng")))
    rows = df_fixed.collect()
    assert rows[0]["lat"] == 10.0
    assert rows[0]["lng"] == 20.0
    assert rows[1]["lat"] == 1.0
    assert rows[1]["lng"] == 2.0

def test_aggregate_weather(spark):
    # Test average aggregation by geohash
    df = spark.createDataFrame([
        Row(geohash="abcd", avg_tmpr_c=10),
        Row(geohash="abcd", avg_tmpr_c=20),
        Row(geohash="efgh", avg_tmpr_c=30)
    ])
    df_agg = df.groupBy("geohash").agg(avg("avg_tmpr_c").alias("avg_temp"))
    result = {row["geohash"]: row["avg_temp"] for row in df_agg.collect()}
    assert result["abcd"] == 15
    assert result["efgh"] == 30

def test_join_restaurant_weather(spark):
    # Sample restaurants
    df_rest = spark.createDataFrame([
        Row(name="A", geohash="abcd"),
        Row(name="B", geohash="efgh"),
        Row(name="C", geohash="ijkl")  # no matching weather
    ])
    # Sample weather
    df_weather = spark.createDataFrame([
        Row(geohash="abcd", avg_temp=15),
        Row(geohash="efgh", avg_temp=30)
    ])
    # Left join
    df_joined = df_rest.join(df_weather, on="geohash", how="left")
    rows = {row["name"]: row["avg_temp"] for row in df_joined.collect()}
    assert rows["A"] == 15
    assert rows["B"] == 30
    assert rows["C"] is None  # No matching weather data
