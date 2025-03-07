FROM golang:1.23-alpine3.21

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN go build
RUN ls
CMD "./worker"