FROM python:3.5-alpine@sha256:179992e913f024340db6347446966f69c153de72ad440b72bf7418c940c8692a

ADD . /usr/src/app
WORKDIR /usr/src/app

RUN pip install -e /usr/src/app

ENTRYPOINT ["riak-exporter"]
