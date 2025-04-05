FROM apache/spark-py

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

USER root

WORKDIR /opt/application

COPY ./analysis.py .
COPY ./requirements.txt .

RUN pip install -r  requirements.txt

CMD ["python3", "analysis.py"]
