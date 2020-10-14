FROM python:3.8

RUN pip3 install pyarrow==1.0.1 numpy==1.19.0 pandas==1.1.3 joblib==0.17.0
COPY benchmark-csv.py /tmp/benchmark-csv.py

CMD python3 /tmp/benchmark-csv.py

