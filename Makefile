GOMOD=$(shell test -f "go.work" && echo "readonly" || echo "vendor")
LDFLAGS=-s -w

cli:
	go build -mod $(GOMOD) -ldflags="$(LDFLAGS)" -o bin/update-hierarchies cmd/update-hierarchies/main.go
	go build -mod $(GOMOD) -ldflags="$(LDFLAGS)" -o bin/pip cmd/pip/main.go
