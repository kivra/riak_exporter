FROM python:3.14.4-alpine@sha256:ccea73754fbcefbef8c2a2a64d902b95ffdde498b3ff8a644fe905a6efecfd41

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
