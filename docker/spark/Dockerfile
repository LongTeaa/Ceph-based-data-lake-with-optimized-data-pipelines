FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir boto3==1.34.162 botocore==1.34.162

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark
