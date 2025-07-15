FROM python:3.13-alpine@sha256:9b4929a72599b6c6389ece4ecbf415fd1355129f22bb92bb137eea098f05e975

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
