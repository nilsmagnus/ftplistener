FROM alpine:3.7
COPY ftplistener ftplistener
ENV GRIBDIR=/data/gribfiles

CMD ./ftplistener -destination $GRIBDIR
