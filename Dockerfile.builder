# vi: ft=dockerfile

FROM golang:latest

RUN mkdir -p /go/src/github.com/jeffrom/logd
WORKDIR /go/src/github.com/jeffrom/logd

VOLUME ./build/logd

# disable cross-compilation
ENV CGO_ENABLED=0

# linux only
ENV GOOS=linux

COPY . /go/src/github.com/jeffrom/logd

# build the binary with debug information removed
CMD go build  -ldflags '-w -s' -a -installsuffix cgo -o build/logd ./cmd/logd
