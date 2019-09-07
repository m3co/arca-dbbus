FROM golang:alpine

RUN apk add alpine-sdk

RUN mkdir -p /go/src/github.com/m3co/arca-jsonrpc/
WORKDIR /go/src/github.com/m3co/arca-jsonrpc/

RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u github.com/go-delve/delve/cmd/dlv

COPY Gopkg.lock .
COPY Gopkg.toml .

RUN ln -s ~/go/bin/dep /bin/
RUN ln -s ~/go/bin/dlv /bin/
RUN dep ensure --vendor-only
