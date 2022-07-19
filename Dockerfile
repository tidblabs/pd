FROM golang:1.18.3-bullseye as builder

RUN apt update && apt install -y make git curl gcc g++ unzip

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN make

FROM debian:bullseye-20220711-slim
RUN apt update && apt install -y jq bash curl && rm /bin/sh && ln -s /bin/bash /bin/sh && apt-get clean

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover
COPY --from=builder /jq /usr/local/bin/jq

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
