FROM gcr.io/distroless/static-debian12:nonroot
# TARGETARCH is set automatically when using BuildKit
ARG TARGETARCH
COPY .bin/linux-${TARGETARCH}/francis /bin
HEALTHCHECK CMD ["/bin/francis", "healthcheck"]
CMD ["/bin/francis"]
ENTRYPOINT ["/bin/francis"]
