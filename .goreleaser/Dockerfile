FROM alpine

RUN mkdir -p /opt/logd && \
    mkdir -p /opt/tmp && \
    addgroup loguser && \
    adduser -S -D -H -h /logd -G loguser loguser && \
    chown loguser:loguser /opt/logd && \
    chmod -R 777 /opt/tmp

COPY logd /usr/local/bin/logd

USER loguser

ENTRYPOINT ["logd"]
