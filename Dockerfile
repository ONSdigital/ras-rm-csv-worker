FROM --platform=$BUILDPLATFORM golang:1.25-alpine

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN go build
RUN ls
CMD "./worker"
