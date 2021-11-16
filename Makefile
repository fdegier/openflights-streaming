BATCH_FILE = data/batch/routes.dat
ROUTES_URL = https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat
IMAGE_NAME = openflights-spark

build:
	docker build . -t $(IMAGE_NAME) --build-arg requirements=requirements.dev.txt

debug-container:
	docker run -it -v $(shell pwd)/data:/app/data --entrypoint /bin/bash $(IMAGE_NAME)

get-data:
	curl -o $(BATCH_FILE) $(ROUTES_URL)

generate-stream:
	bash generate-stream-data.sh

top-10-source-airport-batch: build
	docker run -v $(shell pwd)/data:/app/data $(IMAGE_NAME) --top-n-batch -n 10

top-10-source-airport-stream: build get-data
	docker run -v $(shell pwd)/data:/app/data $(IMAGE_NAME) --top-n-stream -n 10

top-10-source-airport-stream-window: build
	docker run -v $(shell pwd)/data:/app/data $(IMAGE_NAME) --top-n-stream-window -n 10

test:
	pytest tests

clean:
	rm -rf data/top_10*
	rm -rf data/stream/*.dat
	rm -rf data/batch/*.dat

.PHONY: all test clean
