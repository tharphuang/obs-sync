#!/bin/sh
set -e

clean_file() {
  rm -rf build
  mkdir -p build/bin
}

build() {
  basePath="${PWD}"
  go env -w GOOS=linux GOARCH=amd64

  cd "${basePath}"/cmd/client
  go build -ldflags="-s -w" -o client
  cd -
  mv "${basePath}"/cmd/client/client build/bin/

  cd "${basePath}"/cmd/server
  go build -ldflags="-s -w" -o server
  cd -
  mv "${basePath}"/cmd/server/server build/bin/

  go build -ldflags="-s -w" -o import "${basePath}/cmd/command"
  mv "${basePath}"/import build/bin/

  cp "${basePath}"/README.md build/README.md
}

# main function
echo "check binary file is exist!"
clean_file
echo "rebuild file!"
build
