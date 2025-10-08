FROM python:3.14-alpine@sha256:961b74dee425a9c6446d4cc249a1337bf2f6f762865662c1e4f05a84de4940fd

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
