FROM golang:alpine

RUN apk add alpine-sdk

RUN mkdir -p /go/src/github.com/m3co/arca-dbbus/
WORKDIR /go/src/github.com/m3co/arca-dbbus/

RUN go get -v github.com/golang/dep/cmd/dep
RUN go get -v github.com/go-delve/delve/cmd/dlv

COPY Gopkg.lock .
COPY Gopkg.toml .

RUN ln -s ~/go/bin/dep /bin/
RUN ln -s ~/go/bin/dlv /bin/
RUN dep ensure --vendor-only

COPY . .

CMD [ "go", "test", "-v" ]