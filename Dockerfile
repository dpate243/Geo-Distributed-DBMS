FROM python:3.8-slim

WORKDIR /workdir

COPY . .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["/bin/bash"]