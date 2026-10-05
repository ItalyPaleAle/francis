#!/bin/sh

set -e

GO_VERSION="1.27.1"

# The Vercel build image does not include Go, so download the official release and check it against its published checksum
ensure_go() {
    if command -v go >/dev/null 2>&1; then
        return 0
    fi

    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"

    case "$arch" in
        x86_64|amd64)
            arch="amd64"
            ;;
        arm64|aarch64)
            arch="arm64"
            ;;
        *)
            echo "Unsupported architecture for Go bootstrap: $arch" >&2
            exit 1
            ;;
    esac

    install_dir="$PWD/.cache/go-toolchain/go${GO_VERSION}"
    go_bin="$install_dir/go/bin/go"

    if [ ! -x "$go_bin" ]; then
        archive="go${GO_VERSION}.${os}-${arch}.tar.gz"
        url="https://dl.google.com/go/$archive"
        tmp_dir="$PWD/.cache/go-toolchain/tmp"

        echo "Installing Go $GO_VERSION"
        rm -rf "$tmp_dir" "$install_dir"
        mkdir -p "$tmp_dir" "$install_dir"
        curl -fsSL "$url" -o "$tmp_dir/$archive"
        echo "$(curl -fsSL "$url.sha256")  $tmp_dir/$archive" | sha256sum -c -
        tar -C "$install_dir" -xzf "$tmp_dir/$archive"
        rm -rf "$tmp_dir"
    fi

    export PATH="$install_dir/go/bin:$PATH"
}

export GOCACHE="$PWD/.cache/go-build"

# This folder is its own module, and the repository's go.work would make Go resolve every other module in the workspace too
export GOWORK=off

ensure_go
go version

# Vercel serves the public folder as is, so it holds nothing but the generated index
rm -rf public
go run . -out public/index.yaml
