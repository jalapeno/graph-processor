REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest
.PHONY: all graph-processor container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: graph-processor

graph-processor:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-graph-processor

graph-processor-container: graph-processor
	docker build -t $(REGISTRY_NAME)/graph-processor:$(IMAGE_VERSION) -f ./build/Dockerfile.graph-processor .

push: graph-processor-container
	docker push $(REGISTRY_NAME)/graph-processor:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
