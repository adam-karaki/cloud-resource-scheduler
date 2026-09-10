run:
	go run ./cmd/scheduler

test:
	go test ./...

vet:
	go vet ./...

build:
	go build -o bin/cloud-resource-scheduler ./cmd/scheduler

fmt:
	gofmt -w $$(find . -name '*.go' -type f)
