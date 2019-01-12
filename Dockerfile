
FROM alpine:3.7

RUN apk update
RUN apk add nano
RUN apk add curl

ADD ftplistener /ftplistener
ENTRYPOINT ["./ftplistener"]
