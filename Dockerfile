FROM golang:1.13.8 as builder

RUN mkdir /build
WORKDIR /build

COPY go.mod /build/
COPY go.sum /build/
RUN go mod download

COPY . /build
RUN CGO_ENABLED=0 go build -o logd.bin ./cmd/logd

FROM alpine

RUN mkdir -p /opt/logd && \
    mkdir -p /opt/tmp && \
    chmod -R 777 /opt/tmp

COPY --from=builder /build/logd.bin /usr/local/bin/logd

ENTRYPOINT ["logd"]
