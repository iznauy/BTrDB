FROM golang:1.14 AS builder
COPY . BTrDB
WORKDIR BTrDB
ENV GO111MODULE on
ENV GOPROXY https://mirrors.aliyun.com/goproxy/
ENV GOPRIVATE github.com
RUN go build -o /bin/BTrDB


FROM ubuntu:18.04
EXPOSE 2333
COPY --from=builder /bin/BTrDB .
COPY --from=builder /go/BTrDB/btrdb.conf .
COPY --from=builder /go/BTrDB/start.sh .
CMD ["./start.sh"]