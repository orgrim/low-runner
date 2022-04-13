PROG=low-runner
LDFLAGS=-ldflags="-s -w"

TAG?=$(shell git tag -l 'v*' | sort -V | tail -1 | sed -e s,^v,,)
REGISTRY?=orgrim
IMAGE?=low-runner

DOCKER_IMAGE_VERS=$(REGISTRY)/$(IMAGE):$(TAG)
DOCKER_IMAGE_LATEST=$(REGISTRY)/$(IMAGE):latest

all: $(PROG)

$(PROG): *.go
	CGO_ENABLED=0 go build $(LDFLAGS) .

install: $(PROG)
	CGO_ENABLED=0 go install $(LDFLAGS) .

clean:
	-rm $(PROG)

docker-build: Dockerfile
	docker build -t $(DOCKER_IMAGE_VERS) .

docker-push: docker-build
	docker push $(DOCKER_IMAGE_VERS)

docker-push-latest: docker-push
	docker image tag $(DOCKER_IMAGE_VERS) $(DOCKER_IMAGE_LATEST)
	docker push $(DOCKER_IMAGE_LATEST)

.PHONY: all install clean docker-build docker-push docker-push-latest
