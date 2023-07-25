FROM ubuntu:latest

RUN apt update && apt-get upgrade
RUN apt install python3 -y
RUN apt get python3-pip
RUN pip install -r requirements.txt

WORKDIR /app

COPY . /app

CMD 'python3' ./app.py