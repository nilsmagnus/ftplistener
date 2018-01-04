build: main.go
	@go build

docker: build
	cp ftplistener docker
	docker build docker/
