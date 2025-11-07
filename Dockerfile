FROM golang:1.25-alpine

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build
RUN ls
CMD "./worker"
