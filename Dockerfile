FROM python:3.15.2-alpine@sha256:0f9552bb28711333da16915ec146f5e6bb44a7fdac9ced15dc17c2e0206346dd

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
