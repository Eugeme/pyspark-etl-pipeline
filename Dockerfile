FROM python:3.8

ADD pipeline.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "./pipeline.py"]
