import argparse

from loguru import logger

from openflights import OpenFlights

parser = argparse.ArgumentParser()
parser.add_argument("--top-n-batch", dest="top_n_batch", action="store_true", required=False,
                    help="Create a top N of airports used as source")
parser.add_argument("--top-n-stream", dest="top_n_stream", action="store_true", required=False,
                    help="Create a top N of airports used as source")
parser.add_argument("--top-n-stream-window", dest="top_n_stream_window", action="store_true", required=False,
                    help="Create a top N of airports used as source")
parser.add_argument("-n", dest="top_n", default=10, type=int, required=False)
args = parser.parse_args()

of = OpenFlights()

if args.top_n_batch:
    of.create_top_n_source(top_n=args.top_n)
elif args.top_n_stream:
    of.stream_top_n_source(top_n=args.top_n)
elif args.top_n_stream_window:
    of.top_n_stream_window(top_n=args.top_n)
else:
    logger.critical(f"No (valid) flag given for action")
