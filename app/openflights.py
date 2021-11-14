import os

import pyspark.sql.functions as f
import pyspark.sql.types as t
from loguru import logger
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class OpenFlights:
    def __init__(self):
        self.spark = SparkSession.builder.appName('open-flights').master("local[4]").getOrCreate()

        self.routes_schema = t.StructType([t.StructField("Airline", t.StringType(), True),
                                           t.StructField("Airline_ID", t.IntegerType(), True),
                                           t.StructField("Source_airport", t.StringType(), True),
                                           t.StructField("Source_airport_ID", t.IntegerType(), True),
                                           t.StructField("Destination_airport", t.StringType(), True),
                                           t.StructField("Destination_airport_ID", t.IntegerType(), True),
                                           t.StructField("Codeshare", t.StringType(), True),
                                           t.StructField("Stops", t.IntegerType(), True),
                                           t.StructField("Equipment", t.StringType(), True)
                                           ])

    def create_top_n_source(self, top_n: int, output_location: str = None) -> DataFrame:
        """
        Create a top N of airports used as source and writes it to a Parquet file.
        :param top_n: Integer for number of airports to use
        :param output_location: Path for output
        :return: Spark DataFrame
        """
        if not output_location:
            output_location = f"data/top_{top_n}_source_airport.parquet"

        # For the batch job I decided to fetch the data directly from the source
        url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
        logger.info(f"Reading data from GitHub into DF: {url}")
        self.spark.sparkContext.addFile(url)

        df = self.spark.read.csv(os.path.join("file://", SparkFiles.get("routes.dat")), header=False,
                                 schema=self.routes_schema) \
            .groupBy("Source_airport") \
            .count() \
            .orderBy(f.col("count").desc()) \
            .limit(top_n)

        logger.info(f"Top {top_n} source airports:")
        df.show()

        logger.info(f"Writing Top {top_n} source airports to: {output_location}")
        df.coalesce(1).write.mode("overwrite").parquet(output_location)
        return df

    def manipulate_streaming_schema(self) -> None:
        """
        Since the routes data is not really suitable for streaming, I've added a timestamp to the data, however for some
        reason I could not get AWK to keep the columns and append the timestamp, it did work on other sets. Anyways, to
        not further burn time I decided to overwrite an existing column, therefore the schema needs to be updated.
        :return: None, actions are in place
        """
        self.routes_schema.fields.pop(-1)
        self.routes_schema.add(t.StructField("datetime", t.TimestampType(), True))

    def create_stream(self, location: str = "data/batch/*.dat") -> DataFrame:
        """
        Create a Spark DataFrame used for streaming
        :param location: Location of the data used for streaming
        :return: Spark DataFrame
        """
        return self.spark \
            .readStream \
            .option("header", False) \
            .option("cleanSource", "delete") \
            .schema(self.routes_schema) \
            .csv(location)

    def stream_top_n_source(self, top_n: int) -> None:
        """
        Stream top N airports used as source and display the result in the console
        :param top_n: Integer for number of airports to use
        :return: None, only displays in console
        """
        self.manipulate_streaming_schema()
        streaming_df = self.create_stream()

        stream = streaming_df \
            .groupBy("Source_airport") \
            .count() \
            .orderBy(f.col("count").desc()) \
            .limit(top_n) \
            .writeStream \
            .outputMode("complete") \
            .format("console")

        stream.start()\
            .awaitTermination()

    # noinspection PyUnusedLocal
    @staticmethod
    def save_top_10_data(df: DataFrame, *args, **kwargs) -> None:
        """
        Save Spark DataFrame to a local output.
        :param df: Spark DataFrame
        :return: None
        """
        # To illustrate the streaming
        logger.info(f"Writing file")
        df.show()
        df.coalesce(1).write.mode("overwrite").parquet(f"data/top_10_window.parquet")

    def top_n_stream_window(self, top_n: int = 10) -> None:
        """
        Stream the top N airports used as source and save it to a local parquet
        :param top_n: Integer for number of airports to use
        :return: None
        """
        self.manipulate_streaming_schema()
        streaming_df = self.create_stream(location="data/stream/*.dat")

        # Short windows to stimulate streaming in conjunction with the bash script
        stream = streaming_df \
            .groupBy(f.window(streaming_df.datetime, windowDuration="15 seconds", slideDuration="15 seconds"),
                     "Source_airport") \
            .count() \
            .orderBy(f.col("count").desc()) \
            .limit(top_n) \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(self.save_top_10_data) \
            .start()

        stream.awaitTermination()

