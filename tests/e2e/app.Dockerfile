FROM gcr.io/distroless/static-debian13:nonroot
ARG E2E_APP_BINARY
COPY ${E2E_APP_BINARY} /bin/app
ENTRYPOINT ["/bin/app"]
