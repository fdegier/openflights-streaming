from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.types as t

from app.openflights import OpenFlights

of = OpenFlights()


def test_init():
    assert type(of.spark) == SparkSession
    assert type(of.routes_schema) == t.StructType


def test_create_top_n_source(top_n=10):
    df = of.create_top_n_source(top_n=top_n)
    assert df.columns == ['Source_airport', 'count']
    assert df.count() == 10


def test_manipulate_streaming_schema():
    assert of.routes_schema.fields[-1].name == "Equipment"
    of.manipulate_streaming_schema()
    assert of.routes_schema.fields[-1].name == "datetime"


def test_create_stream():
    df = of.create_stream()
    assert type(df) == DataFrame
    assert df.columns == ['Airline', 'Airline_ID', 'Source_airport', 'Source_airport_ID', 'Destination_airport',
                          'Destination_airport_ID', 'Codeshare', 'Stops', 'datetime']


def test_stream_top_n_source():
    # This function is already printing to console, a debug method, not going to test this.
    pass


def test_top_n_stream_window():
    # Ideally this function would have been split up into a more complicated program that can create streams (function)
    # and another function that will start and end streams. Since I didn't go that route for this exercise it will be a
    # very difficult to test this function.
    pass
