.PHONY: build clean

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/DynamoDB DynamoDB/main.go

clean:
	rm -rf ./bin