FROM golang:1.12 as builder

RUN mkdir /build
WORKDIR /build

COPY go.mod /build/
COPY go.sum /build/
RUN go mod download

COPY . /build
RUN CGO_ENABLED=0 go build -o logd.bin ./cmd/logd

FROM alpine

RUN addgroup loguser && \
    adduser -S -D -H -h /logd -G loguser loguser && \
    mkdir /logd && \
    chown loguser:loguser /logd
USER loguser

COPY --from=builder /build/logd.bin /logd/logd

WORKDIR /logd

ENTRYPOINT ["./logd"]
