FROM python:3.8

ADD data_downloader.py .
ADD config.py .
ADD sql_server.py .

ENV PORT 8080
ENV HOST 0.0.0.0

RUN pip install google-cloud-bigquery pandas requests pyarrow

CMD ["python", "./data_downloader.py"]