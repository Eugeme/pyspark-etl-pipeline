FROM datamechanics/spark:3.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

ADD scripts/pipeline.py .
ADD scripts/utils.py .

COPY requirements.txt .

CMD ["python", "./pipeline.py"]
