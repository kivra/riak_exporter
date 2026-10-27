FROM python:3.14.6-alpine@sha256:26730869004e2b9c4b9ad09cab8625e81d256d1ce97e72df5520e806b1709f92

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
