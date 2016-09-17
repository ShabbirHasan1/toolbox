FROM python:3.5
MAINTAINER skeang@gmail.com

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip3 install -vr requirements.txt

# Test dependencies
COPY requirements_dev.txt .
RUN pip3 install -vr requirements_dev.txt

CMD py.test
