FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables sqlite3 \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml /src/
COPY src /src/src
RUN pip install --no-cache-dir /src
WORKDIR /data
