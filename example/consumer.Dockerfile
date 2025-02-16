FROM golang:1.22-alpine AS builder

ENV PATH="/go/bin:${PATH}"
ENV GO111MODULE=on
ENV CGO_ENABLED=1
ENV GOOS=linux

WORKDIR /go/src

COPY . .

WORKDIR /go/src/example
RUN go mod download

RUN apk -U add ca-certificates
RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo
RUN apk update && apk add librdkafka-dev

RUN go build -tags musl --ldflags "-extldflags -static" -o consumer ./consumer

FROM scratch AS runner

COPY --from=builder /go/src/example/consumer /

ENTRYPOINT ["./consumer"]