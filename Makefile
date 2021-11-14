IMAGE_NAME = openflights-spark

build:
	docker build . -t $(IMAGE_NAME)

.PHONY: all test clean
