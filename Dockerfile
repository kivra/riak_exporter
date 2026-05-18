FROM python:3.14.5-alpine@sha256:5a824eb82cc75361f98611f3cfc5091ea33f10a6ccea4d4ebdabbc523b9a1614

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
