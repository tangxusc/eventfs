FROM debian:12-slim

ARG EVENTFS_ARCHIVE=.docker-artifacts/eventfs-linux-native.tar.gz

COPY ${EVENTFS_ARCHIVE} /tmp/eventfs.tar.gz

RUN set -eux; \
    mkdir -p /tmp/eventfs /var/lib/eventfs; \
    tar -xzf /tmp/eventfs.tar.gz -C /tmp/eventfs --strip-components=1; \
    install -m 0755 /tmp/eventfs/eventstored /usr/local/bin/eventstored; \
    install -m 0755 /tmp/eventfs/esctl /usr/local/bin/esctl; \
    chown 65532:65532 /var/lib/eventfs; \
    rm -rf /tmp/eventfs /tmp/eventfs.tar.gz

USER 65532:65532
WORKDIR /var/lib/eventfs

CMD ["eventstored", "--config", "/etc/eventfs/config.toml"]
