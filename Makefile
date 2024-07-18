PROJECT=kuiper-nexmark-test

default: build

build:
	go build -v -o $(PROJECT) ./main.go

clean:
	rm -f $(PROJECT)