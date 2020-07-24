FROM golang:1.14.4-alpine3.12

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN go build
RUN ls
CMD "./worker"