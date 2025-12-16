Spark Project - https://github.com/TimurMMD/Epam/tree/main/Spark

This project was developed using PySpark on Ubuntu Linux.

The weather data was downloaded and reorganized into appropriate folder directories for efficient processing. After preparing the data, it was uploaded into Spark and left-joined with the restaurant dataset using 4-character geohashes.


The following unit tests were made:

def test_geohash_udf_valid(spark):
    """
    Test geohash_4 UDF with valid latitude and longitude values.
    Purpose: Ensure that the UDF returns a 4-character geohash string 
    for normal input coordinates.
    """

def test_geohash_udf_none(spark):
    """
    Test geohash_4 UDF with None values for latitude and longitude.
    Purpose: Verify that the UDF handles missing inputs correctly
    and returns None instead of throwing an error.
    """

def test_fill_missing_coordinates(spark):
    """
    Test filling missing latitude and longitude values in a DataFrame.
    Purpose: Check that rows with missing coordinates are correctly
    updated with new values, while existing coordinates remain unchanged.
    """

def test_aggregate_weather(spark):
    """
    Test weather aggregation by geohash.
    Purpose: Ensure that average temperature calculations per geohash
    are correct, using a small sample dataset.
    """

def test_join_restaurant_weather(spark):
    """
    Test joining restaurant data with aggregated weather data.
    Purpose: Verify that a left join preserves all restaurant rows,
    matches weather averages correctly, and leaves None where
    no matching weather data exists.
    """


After the unit test the only failed test was for test_geohash_udf_none and it's logical as Spark can't operate with None value here, that's why in the project, the dataset was checked for missing values and filled before applying geohash function
