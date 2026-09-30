FROM gcr.io/distroless/static-debian13:nonroot
# TARGETARCH is set automatically when using BuildKit
ARG TARGETARCH
COPY .bin/linux-${TARGETARCH}/francis /bin
# WebTransport over HTTP/3 for hosts
EXPOSE 7400/udp
HEALTHCHECK CMD ["/bin/francis", "healthcheck"]
ENTRYPOINT ["/bin/francis"]
