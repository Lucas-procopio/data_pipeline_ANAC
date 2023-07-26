FROM ubuntu:latest
LABEL main=Lucas

RUN apt update && \
    apt-get upgrade && \
    apt-get install -y

COPY *.py /app/
COPY /credentials/ /app/credentials/
COPY requirements.txt /app/
WORKDIR /app

RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
RUN pip install -r requirements.txt

CMD 'python3' $CREDENTIALS_PATH $DATALAKE $TABLE_ID