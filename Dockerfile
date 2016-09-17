FROM python:3.5
MAINTAINER skeang@gmail.com

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip3 install -vr requirements.txt
CMD py.test
