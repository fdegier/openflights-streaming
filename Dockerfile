FROM python:3.9.6

ARG requirements=requirements.txt

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
COPY requirements.dev.txt .

RUN pip install --no-cache-dir -r $requirements

COPY app app

ENTRYPOINT ["python", "app/main.py"]
