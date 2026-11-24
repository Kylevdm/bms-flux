# glibc python:slim is set directly; Supervisor 2026.04.0+ no longer passes
# BUILD_FROM. The image is built on the box by the Supervisor and by compose.
FROM python:3.13-slim-bookworm

ARG BUILD_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && pip install --no-cache-dir \
        "bms-flux @ git+https://github.com/Kylevdm/bms-flux@v${BUILD_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

COPY run.sh /
RUN chmod a+x /run.sh
CMD [ "/run.sh" ]
