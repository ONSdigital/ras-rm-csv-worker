FROM golang:1.19.2-alpine3.16

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN go build
RUN ls
CMD "./worker"