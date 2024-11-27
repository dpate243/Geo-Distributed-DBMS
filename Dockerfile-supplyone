FROM python:3.8-slim

WORKDIR /workdir

RUN mkdir utils

COPY utils/ .
COPY requirements.txt .
COPY main.py .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["/bin/bash"]