FROM alpine:3.7

RUN apk add --no-cache bash

COPY ftplistener /ftplistener

ENV GRIBDIR=/data/gribfiles

CMD ./ftplistener -destination $GRIBDIR
