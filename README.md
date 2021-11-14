# Schiphol Spark assessment

This repo provides the following results on OpenFlights data:

1. Create top 10 of source airports
2. Create a streaming top 10 of source airports
3. Create a streaming top 10 of source airports over a sliding window

## Prerequisites

In order to run the code you will need to have either `Docker` installed or some form of java, in the repo `openjdk` was
used, if you run it locally uou also need `python3.9.7`.

## General concepts

The runtime environment was designed to be lightweight and minimalistic, performance of Spark was not considered as its
small data and just for demonstration purposes. The `Docker` image was designed to be the preferred environment but
should you choose so, you can also run it locally. Furthermore, the commands used to generate the solution are all
encapsulated in the `Makefile` and can be used as shortcuts, you can however run it in any way you like, just note that
it was designed and tested for Docker.

The folder `./data` was created as a structure to share data between the container and locally, allowing the bash script
`generate-stream-data.sh` to create a stream of data while the container is running.

For local development, including all the make commands, a local build is used, the only difference here is in the
requirements. Anything that is used for production is stored in `requirements.txt` and packages used for testing,
linting etc. are stored in `requirements.dev.txt`

### Gotchas

- Solution was developed on Apple Silicon, I haven't tested it on other architectures
- Spark streaming has a fair bit of new functionality that I wasn't aware of, it was a nice learning excercise as I
  choose to use the latest version of Spark
- This is just a demonstration repo, further testing, optimizations, config based variables, etc. would all be needed in
  a real-life situation

## Solutions

### Top 10 airports used as source - batch

In essence the data is loaded from GitHub (to demonstrate other ways than loading local files), grouped by source
airport and aggregated on count, then written to a local parquet file.

To get the top 10 airports used as source airport, just run the following command

```bash
make top-10-source-airport-batch
```

Alternatively you can run it directly in Docker, which will also allow you to create any top N:

```bash
docker build . -t openflights-spark
docker run -it -v $(pwd)/data:/app/data openflights-spark --top-n-batch -n 15
```

Or locally with Python:

```bash
python3.9 -m venv venv
source venv/bin/activate
pip install -r requirements.dev.txt
python app/main.py --top-n-batch -n 10
```

The above options work for the other solutions as well, for the sake of this README I will only reference the `make`
commands.

### Top 10 airports used as source - stream

The second solution extends upon the first, making it ready for streaming, note that streaming here should mostly be
considered "once data comes in, the top10 view is refreshed directly" as the data is not generally a candidate for
streaming applications, as most of it is static, not time based.

The top 10 streaming variant reads in the entire `routes.dat` and displays it in the console, since its only 1 file the
streaming demonstration of it is quite limited.

```bash
make top-10-source-airport-stream
```

### Top 10 airports used as source per window - stream

The `routes.dat` data is not time sensitive, nor does it contain a timestamp. For the sake of streaming demonstration, I
have added as timestamp and create new data every 10 seconds. This should demonstrate the ability of the application to
perform a streaming aggregation. In order to demonstrate this you need 2 terminals. This is the only function that was
designed to be `top_n` but has been fixed to `top_n=10` as I had issues with passing along arguments to `forEachBatch`.

```bash
# terminal 1
make top-10-source-airport-stream-window
```

Above will start the streaming application, and it will await new data, using the second terminal and a bash script we
will create new data every 10 seconds. The output is stored in `data/top_10_window.parquet`

In a second terminal run the command to generate streaming data:

```bash
# terminal 2
make generate-stream
```

You will now see the streaming application update the aggregation whenever a new file comes in.

Example:

```
2021-11-14 16:13:48.573 | INFO     | openflights:save_top_10_data:107 - Writing file
+--------------------+--------------+-----+                                     
|              window|Source_airport|count|
+--------------------+--------------+-----+
|{2021-11-14 15:56...|           ATL|   16|
|{2021-11-14 15:56...|           FRA|   15|
|{2021-11-14 15:56...|           LGW|   14|
+--------------------+--------------+-----+

2021-11-14 16:13:58.769 | INFO     | openflights:save_top_10_data:107 - Writing file
+--------------------+--------------+-----+                                     
|              window|Source_airport|count|
+--------------------+--------------+-----+
|{2021-11-14 16:13...|           ATL|   36|
|{2021-11-14 16:13...|           MIA|   18|
|{2021-11-14 16:13...|           LHR|   18|
+--------------------+--------------+-----+
```

## Testing

Some minimal tests were written, since I did not start with a test driven development strategy some parts of the
codebase are very hard / impossible to test. To run the test, simply run one the following commands:

```bash
pytest tests
make tests
```



