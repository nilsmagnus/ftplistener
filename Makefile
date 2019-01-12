.PHONY: docker

build: main.go
	@go build

docker:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ftplistener .
	cp ftplistener docker/
	docker build docker/ -t nilslarsgard/ftplistener
