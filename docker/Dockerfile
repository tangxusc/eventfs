FROM debian:12-slim AS payload

ARG EVENTFS_ARCHIVE=.docker-artifacts/eventfs-linux-native.tar.gz

COPY ${EVENTFS_ARCHIVE} /tmp/eventfs.tar.gz

RUN set -eux; \
    mkdir -p /opt/eventfs; \
    tar -xzf /tmp/eventfs.tar.gz -C /opt/eventfs --strip-components=1; \
    rm /tmp/eventfs.tar.gz

FROM debian:12-slim AS server

COPY --from=payload /opt/eventfs/eventstored /usr/local/bin/eventstored
COPY --from=payload /opt/eventfs/esctl /usr/local/bin/esctl

RUN set -eux; \
    chmod 0755 /usr/local/bin/eventstored /usr/local/bin/esctl; \
    mkdir -p /var/lib/eventfs; \
    chown 65532:65532 /var/lib/eventfs

USER 65532:65532
WORKDIR /var/lib/eventfs

CMD ["eventstored", "--config", "/etc/eventfs/config.toml"]

FROM debian:13-slim AS client

RUN set -eux; \
    apt-get update; \
    apt-get install --no-install-recommends -y ca-certificates fuse3; \
    rm -rf /var/lib/apt/lists/*

COPY --from=payload /opt/eventfs/esctl /usr/local/bin/esctl
COPY --from=payload /opt/eventfs/eventfs-fuse /usr/local/bin/eventfs-fuse

RUN set -eux; \
    chmod 0755 /usr/local/bin/esctl /usr/local/bin/eventfs-fuse; \
    mkdir -p /mnt/eventfs

WORKDIR /mnt/eventfs

CMD ["eventfs-fuse", "mount", "--config", "/etc/eventfs/fuse.toml", "/mnt/eventfs"]
